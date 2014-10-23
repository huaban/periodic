package periodic

import (
    "log"
    "net"
    "time"
    "sync"
    "strings"
    "container/heap"
    "github.com/Lupino/periodic/driver"
    "github.com/Lupino/periodic/protocol"
)


type Sched struct {
    jobTimer   *time.Timer
    grabQueue  *GrabQueue
    procQueue  map[int64]driver.Job
    revertPQ   PriorityQueue
    revTimer   *time.Timer
    entryPoint string
    JobLocker  *sync.Mutex
    stats      map[string]*FuncStat
    FuncLocker *sync.Mutex
    driver     driver.StoreDriver
    jobPQ      map[string]*PriorityQueue
    PQLocker   *sync.Mutex
    timeout    time.Duration
    alive      bool
    cacheItem  *Item
}


func NewSched(entryPoint string, store driver.StoreDriver, timeout time.Duration) *Sched {
    sched := new(Sched)
    sched.jobTimer = time.NewTimer(1 * time.Hour)
    sched.revTimer = time.NewTimer(1 * time.Hour)
    sched.grabQueue = NewGrabQueue()
    sched.procQueue = make(map[int64]driver.Job)
    sched.revertPQ = make(PriorityQueue, 0)
    heap.Init(&sched.revertPQ)
    sched.entryPoint = entryPoint
    sched.JobLocker = new(sync.Mutex)
    sched.PQLocker = new(sync.Mutex)
    sched.FuncLocker = new(sync.Mutex)
    sched.stats = make(map[string]*FuncStat)
    sched.driver = store
    sched.jobPQ = make(map[string]*PriorityQueue)
    sched.timeout = timeout
    sched.alive = true
    sched.cacheItem = nil
    return sched
}


func (sched *Sched) Serve() {
    parts := strings.SplitN(sched.entryPoint, "://", 2)
    if parts[0] == "unix" {
        sockCheck(parts[1])
    }
    sched.loadJobQueue()
    go sched.handleJobPQ()
    go sched.handleRevertPQ()
    listen, err := net.Listen(parts[0], parts[1])
    if err != nil {
        log.Fatal(err)
    }
    defer listen.Close()
    log.Printf("Periodic task system started on %s\n", sched.entryPoint)
    for {
        if !sched.alive {
            break
        }
        conn, err := listen.Accept()
        if err != nil {
            log.Fatal(err)
        }
        if sched.timeout > 0 {
            conn.SetDeadline(time.Now().Add(sched.timeout * time.Second))
        }
        sched.HandleConnection(conn)
    }
}


func (sched *Sched) NotifyJobTimer() {
    sched.jobTimer.Reset(time.Millisecond)
}


func (sched *Sched) NotifyRevertTimer() {
    sched.revTimer.Reset(time.Millisecond)
}


func (sched *Sched) HandleConnection(conn net.Conn) {
    c := Conn{Conn: conn}
    payload, err := c.Receive()
    if err != nil {
        return
    }
    switch protocol.ClientType(payload[0]) {
    case protocol.TYPE_CLIENT:
        client := NewClient(sched, c)
        go client.Handle()
        break
    case protocol.TYPE_WORKER:
        worker := NewWorker(sched, c)
        go worker.Handle()
        break
    default:
        log.Printf("Unsupport client %d\n", payload[0])
        c.Close()
        break
    }
}


func (sched *Sched) Done(jobId int64) {
    defer sched.NotifyJobTimer()
    defer sched.NotifyRevertTimer()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    if _, ok := sched.procQueue[jobId]; ok {
        delete(sched.procQueue, jobId)
    }
    job, err := sched.driver.Get(jobId)
    if err == nil {
        sched.driver.Delete(jobId)
        sched.DecrStatJob(job)
        sched.DecrStatProc(job)
        sched.removeRevertPQ(job)
    }
    return
}


func (sched *Sched) SubmitJob(grabItem GrabItem, job driver.Job) bool {
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    if job.Name == "" {
        sched.driver.Delete(job.Id)
        return true
    }
    if _, ok := sched.procQueue[job.Id]; ok {
        return true
    }

    if !grabItem.w.alive {
        return false
    }
    if err := grabItem.w.HandleDo(grabItem.msgId, job); err != nil {
        grabItem.w.alive = false
        return false
    }
    now := time.Now()
    current := int64(now.Unix())
    job.Status = driver.JOB_STATUS_PROC
    job.RunAt = current
    sched.driver.Save(&job)
    sched.IncrStatProc(job)
    sched.pushRevertPQ(job)
    sched.NotifyRevertTimer()
    sched.procQueue[job.Id] = job
    sched.grabQueue.Remove(grabItem)
    return true
}


func (sched *Sched) clearCacheItem() {
    defer sched.PQLocker.Unlock()
    sched.PQLocker.Lock()
    sched.cacheItem = nil
}


func (sched *Sched) lessItem() (lessItem *Item) {
    defer sched.PQLocker.Unlock()
    sched.PQLocker.Lock()
    if sched.cacheItem != nil {
        return sched.cacheItem
    }
    maybeItem := make(map[string]*Item)
    for Func, stat := range sched.stats {
        if stat.Worker == 0 {
            continue
        }
        pq, ok := sched.jobPQ[Func]
        if !ok || pq.Len() == 0 {
            continue
        }

        item := heap.Pop(pq).(*Item)

        maybeItem[Func] = item

    }

    if len(maybeItem) == 0 {
        return nil
    }

    var lessFunc string

    for Func, item := range maybeItem {
        if lessItem == nil {
            lessItem = item
            lessFunc = Func
            continue
        }
        if lessItem.priority > item.priority {
            lessItem = item
            lessFunc = Func
        }
    }

    for Func, item := range maybeItem {
        if Func == lessFunc {
            continue
        }
        pq := sched.jobPQ[Func]
        heap.Push(pq, item)
    }
    sched.cacheItem = lessItem
    return
}


func (sched *Sched) handleJobPQ() {
    var current time.Time
    var timestamp int64
    for {
        if !sched.alive {
            break
        }
        if sched.grabQueue.Len() == 0 {
            sched.jobTimer.Reset(time.Minute)
            current =<-sched.jobTimer.C
            continue
        }

        lessItem := sched.lessItem()

        if lessItem == nil {
            sched.jobTimer.Reset(time.Minute)
            current =<-sched.jobTimer.C
            continue
        }

        schedJob, err := sched.driver.Get(lessItem.value)

        if err != nil {
            sched.clearCacheItem()
            log.Printf("handleJobPQ error job: %d %v\n", lessItem.value, err)
            continue
        }

        timestamp = int64(time.Now().Unix())

        if schedJob.SchedAt > timestamp {
            sched.jobTimer.Reset(time.Second * time.Duration(schedJob.SchedAt - timestamp))
            current =<-sched.jobTimer.C
            timestamp = int64(current.Unix())
            if schedJob.SchedAt > timestamp {
                sched.pushJobPQ(schedJob)
                continue
            }
        }

        grabItem, err := sched.grabQueue.Get(schedJob.Func)
        if err == nil {
            if sched.SubmitJob(grabItem, schedJob) {
                sched.clearCacheItem()
            } else {
                sched.pushJobPQ(schedJob)
            }
        } else {
            sched.pushJobPQ(schedJob)
        }
    }
}


func (sched *Sched) handleRevertPQ() {
    var current time.Time
    var timestamp int64
    for {
        if !sched.alive {
            break
        }
        if sched.revertPQ.Len() == 0 {
            sched.revTimer.Reset(time.Minute)
            current =<-sched.revTimer.C
            continue
        }

        sched.PQLocker.Lock()
        item := heap.Pop(&sched.revertPQ).(*Item)
        sched.PQLocker.Unlock()

        if item == nil {
            sched.revTimer.Reset(time.Minute)
            current =<-sched.revTimer.C
            continue
        }

        revertJob, err := sched.driver.Get(item.value)

        if err != nil {
            log.Printf("handleRevertPQ error: job: %d %v\n", item.value, err)
            continue
        }

        timestamp = int64(time.Now().Unix())

        if item.priority > timestamp {
            sched.revTimer.Reset(time.Second * time.Duration(item.priority - timestamp))
            current =<-sched.revTimer.C
            timestamp = int64(current.Unix())
            if item.priority > timestamp {
                sched.pushRevertPQ(revertJob)
                continue
            }
        }

        sched.DecrStatProc(revertJob)
        revertJob.Status = driver.JOB_STATUS_READY
        sched.driver.Save(&revertJob)
        sched.pushJobPQ(revertJob)
        if _, ok := sched.procQueue[revertJob.Id]; ok {
            delete(sched.procQueue, revertJob.Id)
        }
    }
}


func (sched *Sched) Fail(jobId int64) {
    defer sched.NotifyJobTimer()
    defer sched.NotifyRevertTimer()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    if _, ok := sched.procQueue[jobId]; ok {
        delete(sched.procQueue, jobId)
    }
    job, _ := sched.driver.Get(jobId)
    sched.DecrStatProc(job)
    sched.removeRevertPQ(job)
    job.Status = driver.JOB_STATUS_READY
    sched.driver.Save(&job)
    sched.pushJobPQ(job)
    return
}


func (sched *Sched) getFuncStat(Func string) *FuncStat {
    defer sched.FuncLocker.Unlock()
    sched.FuncLocker.Lock()
    stat, ok := sched.stats[Func]
    if !ok {
        stat = new(FuncStat)
        sched.stats[Func] = stat
    }
    return stat
}


func (sched *Sched) IncrStatFunc(Func string) {
    stat := sched.getFuncStat(Func)
    stat.Worker.Incr()
}


func (sched *Sched) DecrStatFunc(Func string) {
    stat := sched.getFuncStat(Func)
    stat.Worker.Decr()
}


func (sched *Sched) IncrStatJob(job driver.Job) {
    stat := sched.getFuncStat(job.Func)
    stat.Job.Incr()
}


func (sched *Sched) DecrStatJob(job driver.Job) {
    stat := sched.getFuncStat(job.Func)
    stat.Job.Decr()
}


func (sched *Sched) IncrStatProc(job driver.Job) {
    stat := sched.getFuncStat(job.Func)
    if job.Status == driver.JOB_STATUS_PROC {
        stat.Processing.Incr()
    }
}


func (sched *Sched) DecrStatProc(job driver.Job) {
    stat := sched.getFuncStat(job.Func)
    if job.Status == driver.JOB_STATUS_PROC {
        stat.Processing.Decr()
    }
}


func (sched *Sched) SchedLater(jobId int64, delay int64) {
    defer sched.NotifyJobTimer()
    defer sched.NotifyRevertTimer()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    if _, ok := sched.procQueue[jobId]; ok {
        delete(sched.procQueue, jobId)
    }
    job, _ := sched.driver.Get(jobId)
    sched.DecrStatProc(job)
    sched.removeRevertPQ(job)
    job.Status = driver.JOB_STATUS_READY
    var now = time.Now()
    job.SchedAt = int64(now.Unix()) + delay
    sched.driver.Save(&job)
    sched.pushJobPQ(job)
    return
}


func (sched *Sched) pushJobPQ(job driver.Job) bool {
    defer sched.PQLocker.Unlock()
    sched.PQLocker.Lock()
    if job.Status == driver.JOB_STATUS_READY {
        item := &Item{
            value: job.Id,
            priority: job.SchedAt,
        }
        if sched.cacheItem != nil && item.priority < sched.cacheItem.priority {
            if job.Id == sched.cacheItem.value {
                return true
            }
            job, _ = sched.driver.Get(sched.cacheItem.value)
            sched.cacheItem = item
            if job.Id <= 0 || job.Status != driver.JOB_STATUS_READY {
                return false
            }
        }
        pq, ok := sched.jobPQ[job.Func]
        if !ok {
            pq1 := make(PriorityQueue, 0)
            pq = &pq1
            sched.jobPQ[job.Func] = pq
            heap.Init(pq)
        }
        heap.Push(pq, item)
        return true
    }
    return false
}


func (sched *Sched) pushRevertPQ(job driver.Job) {
    defer sched.PQLocker.Unlock()
    sched.PQLocker.Lock()
    if job.Status == driver.JOB_STATUS_PROC && job.Timeout > 0 {
        runAt := job.RunAt
        if runAt == 0 {
            runAt = job.SchedAt
        }
        item := &Item{
            value: job.Id,
            priority: runAt + job.Timeout,
        }
        heap.Push(&sched.revertPQ, item)
    }
}


func (sched *Sched) removeRevertPQ(job driver.Job) {
    defer sched.PQLocker.Unlock()
    sched.PQLocker.Lock()
    if job.Status == driver.JOB_STATUS_PROC && job.Timeout > 0 {
        for _, item := range sched.revertPQ {
            if item.value == job.Id {
                heap.Remove(&sched.revertPQ, item.index)
                break
            }
        }
    }
}


func (sched *Sched) loadJobQueue() {
    updateQueue := make([]driver.Job, 0)
    removeQueue := make([]driver.Job, 0)
    var now = time.Now()
    current := int64(now.Unix())

    iter := sched.driver.NewIterator(nil)
    for {
        if !iter.Next() {
            break
        }
        job := iter.Value()
        if job.Name == "" {
            removeQueue = append(removeQueue, job)
            continue
        }
        sched.IncrStatJob(job)
        sched.pushJobPQ(job)
        runAt := job.RunAt
        if runAt < job.SchedAt {
            runAt = job.SchedAt
        }
        if runAt + job.Timeout < current {
            updateQueue = append(updateQueue, job)
        } else {
            sched.procQueue[job.Id] = job
            sched.IncrStatProc(job)
            sched.pushRevertPQ(job)
        }
    }

    iter.Close()

    for _, job := range updateQueue {
        job.Status = driver.JOB_STATUS_READY
        sched.driver.Save(&job)
    }

    for _, job := range removeQueue {
        sched.driver.Delete(job.Id)
    }
}


func (sched *Sched) Close() {
    sched.alive = false
    sched.driver.Close()
    log.Printf("Periodic task system shutdown\n")
}
