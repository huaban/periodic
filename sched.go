package periodic

import (
    "log"
    "net"
    "time"
    "sync"
    "strings"
    "container/heap"
    "github.com/Lupino/periodic/stat"
    "github.com/Lupino/periodic/queue"
    "github.com/Lupino/periodic/driver"
    "github.com/Lupino/periodic/protocol"
)


type Sched struct {
    jobTimer   *time.Timer
    grabQueue  *grabQueue
    procQueue  map[int64]driver.Job
    revertPQ   queue.PriorityQueue
    revTimer   *time.Timer
    entryPoint string
    jobLocker  *sync.Mutex
    timerLocker *sync.Mutex
    stats      map[string]*stat.FuncStat
    funcLocker *sync.Mutex
    driver     driver.StoreDriver
    jobPQ      map[string]*queue.PriorityQueue
    PQLocker   *sync.Mutex
    timeout    time.Duration
    alive      bool
    cacheItem  *queue.Item
}


func NewSched(entryPoint string, store driver.StoreDriver, timeout time.Duration) *Sched {
    sched := new(Sched)
    sched.jobTimer = time.NewTimer(1 * time.Hour)
    sched.revTimer = time.NewTimer(1 * time.Hour)
    sched.grabQueue = newGrabQueue()
    sched.procQueue = make(map[int64]driver.Job)
    sched.revertPQ = make(queue.PriorityQueue, 0)
    heap.Init(&sched.revertPQ)
    sched.entryPoint = entryPoint
    sched.jobLocker = new(sync.Mutex)
    sched.PQLocker = new(sync.Mutex)
    sched.funcLocker = new(sync.Mutex)
    sched.timerLocker = new(sync.Mutex)
    sched.stats = make(map[string]*stat.FuncStat)
    sched.driver = store
    sched.jobPQ = make(map[string]*queue.PriorityQueue)
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
        sched.handleConnection(conn)
    }
}


func (sched *Sched) notifyJobTimer() {
    sched.resetJobTimer(time.Millisecond)
}


func (sched *Sched) resetJobTimer(d time.Duration) {
    defer sched.timerLocker.Unlock()
    sched.timerLocker.Lock()
    sched.jobTimer.Reset(d)
}


func (sched *Sched) notifyRevertTimer() {
    sched.resetRevertTimer(time.Millisecond)
}


func (sched *Sched) resetRevertTimer(d time.Duration) {
    defer sched.timerLocker.Unlock()
    sched.timerLocker.Lock()
    sched.revTimer.Reset(d)
}


func (sched *Sched) handleConnection(conn net.Conn) {
    c := protocol.Conn{Conn: conn}
    payload, err := c.Receive()
    if err != nil {
        return
    }
    switch protocol.ClientType(payload[0]) {
    case protocol.TYPE_CLIENT:
        client := newClient(sched, c)
        go client.handle()
        break
    case protocol.TYPE_WORKER:
        w := newWorker(sched, c)
        go w.handle()
        break
    default:
        log.Printf("Unsupport client %d\n", payload[0])
        c.Close()
        break
    }
}


func (sched *Sched) done(jobId int64) {
    defer sched.notifyJobTimer()
    defer sched.notifyRevertTimer()
    defer sched.jobLocker.Unlock()
    sched.jobLocker.Lock()
    if _, ok := sched.procQueue[jobId]; ok {
        delete(sched.procQueue, jobId)
    }
    job, err := sched.driver.Get(jobId)
    if err == nil {
        sched.driver.Delete(jobId)
        sched.decrStatJob(job)
        sched.decrStatProc(job)
        sched.removeRevertPQ(job)
    }
    return
}


func (sched *Sched) submitJob(item grabItem, job driver.Job) bool {
    defer sched.jobLocker.Unlock()
    sched.jobLocker.Lock()
    if job.Name == "" {
        sched.driver.Delete(job.Id)
        return true
    }
    if _, ok := sched.procQueue[job.Id]; ok {
        return true
    }

    if !item.w.alive {
        return false
    }
    if err := item.w.handleJobAssign(item.msgId, job); err != nil {
        item.w.alive = false
        return false
    }
    now := time.Now()
    current := int64(now.Unix())
    job.Status = driver.JOB_STATUS_PROC
    job.RunAt = current
    sched.driver.Save(&job)
    sched.incrStatProc(job)
    sched.pushRevertPQ(job)
    sched.notifyRevertTimer()
    sched.procQueue[job.Id] = job
    sched.grabQueue.remove(item)
    return true
}


func (sched *Sched) clearCacheItem() {
    defer sched.PQLocker.Unlock()
    sched.PQLocker.Lock()
    sched.cacheItem = nil
}


func (sched *Sched) lessItem() (lessItem *queue.Item) {
    defer sched.PQLocker.Unlock()
    sched.PQLocker.Lock()
    if sched.cacheItem != nil {
        return sched.cacheItem
    }
    maybeItem := make(map[string]*queue.Item)
    for Func, stat := range sched.stats {
        if stat.Worker.Int() == 0 {
            continue
        }
        pq, ok := sched.jobPQ[Func]
        if !ok || pq.Len() == 0 {
            continue
        }

        item := heap.Pop(pq).(*queue.Item)

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
        if lessItem.Priority > item.Priority {
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
        if sched.grabQueue.len() == 0 {
            sched.resetJobTimer(time.Minute)
            current =<-sched.jobTimer.C
            continue
        }

        lessItem := sched.lessItem()

        if lessItem == nil {
            sched.resetJobTimer(time.Minute)
            current =<-sched.jobTimer.C
            continue
        }

        schedJob, err := sched.driver.Get(lessItem.Value)

        if err != nil {
            sched.clearCacheItem()
            log.Printf("handleJobPQ error job: %d %v\n", lessItem.Value, err)
            continue
        }

        timestamp = int64(time.Now().Unix())

        if schedJob.SchedAt > timestamp {
            sched.resetJobTimer(time.Second * time.Duration(schedJob.SchedAt - timestamp))
            current =<-sched.jobTimer.C
            timestamp = int64(current.Unix())
            if schedJob.SchedAt > timestamp {
                sched.pushJobPQ(schedJob)
                continue
            }
        }

        grabItem, err := sched.grabQueue.get(schedJob.Func)
        if err == nil {
            if sched.submitJob(grabItem, schedJob) {
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
        sched.PQLocker.Lock()
        pqLen := sched.revertPQ.Len()
        sched.PQLocker.Unlock()
        if pqLen == 0 {
            sched.resetRevertTimer(time.Minute)
            current =<-sched.revTimer.C
            continue
        }

        sched.PQLocker.Lock()
        item := heap.Pop(&sched.revertPQ).(*queue.Item)
        sched.PQLocker.Unlock()

        if item == nil {
            sched.resetRevertTimer(time.Minute)
            current =<-sched.revTimer.C
            continue
        }

        revertJob, err := sched.driver.Get(item.Value)

        if err != nil {
            log.Printf("handleRevertPQ error: job: %d %v\n", item.Value, err)
            continue
        }

        timestamp = int64(time.Now().Unix())

        if item.Priority > timestamp {
            sched.resetRevertTimer(time.Second * time.Duration(item.Priority - timestamp))
            current =<-sched.revTimer.C
            timestamp = int64(current.Unix())
            if item.Priority > timestamp {
                sched.pushRevertPQ(revertJob)
                continue
            }
        }

        sched.decrStatProc(revertJob)
        revertJob.Status = driver.JOB_STATUS_READY
        sched.driver.Save(&revertJob)
        sched.pushJobPQ(revertJob)
        sched.jobLocker.Lock()
        if _, ok := sched.procQueue[revertJob.Id]; ok {
            delete(sched.procQueue, revertJob.Id)
        }
        sched.jobLocker.Unlock()
    }
}


func (sched *Sched) fail(jobId int64) {
    defer sched.notifyJobTimer()
    defer sched.notifyRevertTimer()
    defer sched.jobLocker.Unlock()
    sched.jobLocker.Lock()
    if _, ok := sched.procQueue[jobId]; ok {
        delete(sched.procQueue, jobId)
    }
    job, _ := sched.driver.Get(jobId)
    sched.decrStatProc(job)
    sched.removeRevertPQ(job)
    job.Status = driver.JOB_STATUS_READY
    sched.driver.Save(&job)
    sched.pushJobPQ(job)
    return
}


func (sched *Sched) getFuncStat(Func string) *stat.FuncStat {
    defer sched.funcLocker.Unlock()
    sched.funcLocker.Lock()
    st, ok := sched.stats[Func]
    if !ok {
        st = stat.NewFuncStat(Func)
        sched.stats[Func] = st
    }
    return st
}


func (sched *Sched) incrStatFunc(Func string) {
    stat := sched.getFuncStat(Func)
    stat.Worker.Incr()
}


func (sched *Sched) decrStatFunc(Func string) {
    stat := sched.getFuncStat(Func)
    stat.Worker.Decr()
}


func (sched *Sched) incrStatJob(job driver.Job) {
    stat := sched.getFuncStat(job.Func)
    stat.Job.Incr()
}


func (sched *Sched) decrStatJob(job driver.Job) {
    stat := sched.getFuncStat(job.Func)
    stat.Job.Decr()
}


func (sched *Sched) incrStatProc(job driver.Job) {
    stat := sched.getFuncStat(job.Func)
    if job.Status == driver.JOB_STATUS_PROC {
        stat.Processing.Incr()
    }
}


func (sched *Sched) decrStatProc(job driver.Job) {
    stat := sched.getFuncStat(job.Func)
    if job.Status == driver.JOB_STATUS_PROC {
        stat.Processing.Decr()
    }
}


func (sched *Sched) schedLater(jobId int64, delay int64) {
    defer sched.notifyJobTimer()
    defer sched.notifyRevertTimer()
    defer sched.jobLocker.Unlock()
    sched.jobLocker.Lock()
    if _, ok := sched.procQueue[jobId]; ok {
        delete(sched.procQueue, jobId)
    }
    job, _ := sched.driver.Get(jobId)
    sched.decrStatProc(job)
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
        item := &queue.Item{
            Value: job.Id,
            Priority: job.SchedAt,
        }
        if sched.cacheItem != nil && item.Priority < sched.cacheItem.Priority {
            if job.Id == sched.cacheItem.Value {
                return true
            }
            job, _ = sched.driver.Get(sched.cacheItem.Value)
            sched.cacheItem = item
            if job.Id <= 0 || job.Status != driver.JOB_STATUS_READY {
                return false
            }
        }
        pq, ok := sched.jobPQ[job.Func]
        if !ok {
            pq1 := make(queue.PriorityQueue, 0)
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
        item := &queue.Item{
            Value: job.Id,
            Priority: runAt + job.Timeout,
        }
        heap.Push(&sched.revertPQ, item)
    }
}


func (sched *Sched) removeRevertPQ(job driver.Job) {
    defer sched.PQLocker.Unlock()
    sched.PQLocker.Lock()
    if job.Status == driver.JOB_STATUS_PROC && job.Timeout > 0 {
        for _, item := range sched.revertPQ {
            if item.Value == job.Id {
                heap.Remove(&sched.revertPQ, item.Index)
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
        sched.incrStatJob(job)
        sched.pushJobPQ(job)
        runAt := job.RunAt
        if runAt < job.SchedAt {
            runAt = job.SchedAt
        }
        if runAt + job.Timeout < current {
            updateQueue = append(updateQueue, job)
        } else {
            sched.jobLocker.Lock()
            sched.procQueue[job.Id] = job
            sched.jobLocker.Unlock()
            sched.incrStatProc(job)
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
