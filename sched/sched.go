package sched

import (
    "log"
    "net"
    "time"
    "sync"
    "strings"
    "container/list"
)


type Sched struct {
    timer            *time.Timer
    grabQueue        *list.List
    jobQueue         *list.List
    entryPoint       string
    JobLocker        *sync.Mutex
    Funcs            map[string]*FuncStat
    store            Storer
}


type Counter uint

func (c *Counter) Incr() {
    *c = *c + 1
}


func (c *Counter) Decr() {
    *c = *c - 1
}


type FuncStat struct {
    Worker Counter `json:"worker_count"`
    Job    Counter `json:"job_count"`
    Processing     Counter `json:"processing"`
}


func NewSched(entryPoint string, store Storer) *Sched {
    sched := new(Sched)
    sched.timer = time.NewTimer(1 * time.Hour)
    sched.grabQueue = list.New()
    sched.jobQueue = list.New()
    sched.entryPoint = entryPoint
    sched.JobLocker = new(sync.Mutex)
    sched.Funcs = make(map[string]*FuncStat)
    sched.store = store
    return sched
}


func (sched *Sched) Serve() {
    parts := strings.SplitN(sched.entryPoint, "://", 2)
    if parts[0] == "unix" {
        sockCheck(parts[1])
    }
    sched.checkJobQueue()
    go sched.handle()
    listen, err := net.Listen(parts[0], parts[1])
    if err != nil {
        log.Fatal(err)
    }
    defer listen.Close()
    log.Printf("huabot-sched started on %s\n", sched.entryPoint)
    for {
        conn, err := listen.Accept()
        if err != nil {
            log.Fatal(err)
        }
        sched.HandleConnection(conn)
    }
}


func (sched *Sched) Notify() {
    sched.timer.Reset(time.Millisecond)
}


func (sched *Sched) DieWorker(worker *Worker) {
    defer sched.Notify()
    sched.removeGrabQueue(worker)
    worker.Close()
}

func (sched *Sched) HandleConnection(conn net.Conn) {
    c := Conn{Conn: conn}
    payload, err := c.Receive()
    if err != nil {
        return
    }
    switch payload[0] {
    case TYPE_CLIENT:
        client := NewClient(sched, c)
        go client.Handle()
        break
    case TYPE_WORKER:
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
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    job, err := sched.store.Get(jobId)
    if err == nil {
        sched.store.Delete(jobId)
        sched.DecrStatJob(job)
        sched.DecrStatProc(job)
    }
    return
}


func (sched *Sched) isDoJob(job Job) bool {
    now := time.Now()
    current := int64(now.Unix())
    ret := false
    for e := sched.jobQueue.Front(); e != nil; e = e.Next() {
        chk := e.Value.(Job)
        runAt := chk.RunAt
        if runAt < chk.SchedAt {
            runAt = chk.SchedAt
        }
        if chk.Timeout > 0 && runAt + chk.Timeout < current {
            newJob, _ := sched.store.Get(chk.Id)
            if newJob.Status == JOB_STATUS_PROC {
                sched.DecrStatProc(newJob)
                newJob.Status = JOB_STATUS_READY
                sched.store.Save(newJob)
            }
            sched.jobQueue.Remove(e)
            continue
        }
        if chk.Id == job.Id {
            old := e.Value.(Job)
            runAt := old.RunAt
            if runAt < old.SchedAt {
                runAt = old.SchedAt
            }
            if old.Timeout > 0 && runAt + old.Timeout < current {
                ret = false
            } else {
                ret = true
            }
        }
    }
    return ret
}


func (sched *Sched) SubmitJob(worker *Worker, job Job) {
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    if job.Name == "" {
        sched.store.Delete(job.Id)
        return
    }
    if sched.isDoJob(job) {
        return
    }
    if !worker.alive {
        return
    }
    if err := worker.HandleDo(job); err != nil {
        worker.alive = false
        go sched.DieWorker(worker)
        return
    }
    now := time.Now()
    current := int64(now.Unix())
    job.Status = JOB_STATUS_PROC
    job.RunAt = current
    sched.store.Save(job)
    sched.IncrStatProc(job)
    sched.jobQueue.PushBack(job)
    sched.removeGrabQueue(worker)
}


func (sched *Sched) handle() {
    var current time.Time
    var timestamp int64
    var schedJob Job
    var isFirst bool
    for {
        if sched.grabQueue.Len() == 0 {
            sched.timer.Reset(time.Minute)
            current =<-sched.timer.C
            continue
        }

        isFirst = true
        for Func, stat := range sched.Funcs {
            if stat.Worker == 0 || (stat.Job > 0 && stat.Processing == stat.Job) {
                continue
            }
            job, err := sched.store.Next(Func)
            if err != nil {
                continue
            }

            if isFirst {
                schedJob = job
                isFirst = false
                continue
            }

            if schedJob.SchedAt > job.SchedAt {
                schedJob = job
            }
        }
        if isFirst {
            sched.timer.Reset(time.Minute)
            current =<-sched.timer.C
            continue
        }

        timestamp = int64(time.Now().Unix())

        if schedJob.SchedAt > timestamp {
            sched.timer.Reset(time.Second * time.Duration(schedJob.SchedAt - timestamp))
            current =<-sched.timer.C
            timestamp = int64(current.Unix())
            if schedJob.SchedAt > timestamp {
                continue
            }
        }

        isSubmited := false
        for e := sched.grabQueue.Front(); e != nil; e = e.Next() {
            worker := e.Value.(*Worker)
            for _, Func := range worker.Funcs {
                if schedJob.Func == Func {
                    sched.SubmitJob(worker, schedJob)
                    isSubmited = true
                    break
                }
            }
            if isSubmited {
                break
            }
        }

        if !isSubmited {
            sched.DecrStatFunc(schedJob.Func)
        }
    }
}


func (sched *Sched) Fail(jobId int64) {
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    job, _ := sched.store.Get(jobId)
    sched.DecrStatProc(job)
    job.Status = JOB_STATUS_READY
    sched.store.Save(job)
    return
}


func (sched *Sched) IncrStatFunc(Func string) {
    stat, ok := sched.Funcs[Func]
    if !ok {
        stat = new(FuncStat)
        sched.Funcs[Func] = stat
    }
    stat.Worker.Incr()
}


func (sched *Sched) DecrStatFunc(Func string) {
    stat, ok := sched.Funcs[Func]
    if ok {
        stat.Worker.Decr()
    }
}


func (sched *Sched) IncrStatJob(job Job) {
    stat, ok := sched.Funcs[job.Func]
    if !ok {
        stat = new(FuncStat)
        sched.Funcs[job.Func] = stat
    }
    stat.Job.Incr()
}


func (sched *Sched) DecrStatJob(job Job) {
    stat, ok := sched.Funcs[job.Func]
    if ok {
        stat.Job.Decr()
    }
}


func (sched *Sched) IncrStatProc(job Job) {
    stat, ok := sched.Funcs[job.Func]
    if !ok {
        stat = new(FuncStat)
        sched.Funcs[job.Func] = stat
    }
    if job.Status == JOB_STATUS_PROC {
        stat.Processing.Incr()
    }
}


func (sched *Sched) DecrStatProc(job Job) {
    stat, ok := sched.Funcs[job.Func]
    if ok && job.Status == JOB_STATUS_PROC {
        stat.Processing.Decr()
    }
}


func (sched *Sched) SchedLater(jobId int64, delay int64) {
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    job, _ := sched.store.Get(jobId)
    sched.DecrStatProc(job)
    job.Status = JOB_STATUS_READY
    var now = time.Now()
    job.SchedAt = int64(now.Unix()) + delay
    sched.store.Save(job)
    return
}


func (sched *Sched) removeGrabQueue(worker *Worker) {
    for e := sched.grabQueue.Front(); e != nil; e = e.Next() {
        if e.Value.(*Worker) == worker {
            sched.grabQueue.Remove(e)
        }
    }
}


func (sched *Sched) checkJobQueue() {
    start := int64(0)
    limit := int64(20)
    total, _ := sched.store.Count()
    updateQueue := make([]Job, 0)
    removeQueue := make([]Job, 0)
    var now = time.Now()
    current := int64(now.Unix())

    for start = 0; start < total; start += limit {
        jobs, _ := sched.store.GetAll(start, start + limit - 1)
        for _, job := range jobs {
            if job.Name == "" {
                removeQueue = append(removeQueue, job)
                continue
            }
            sched.IncrStatJob(job)
            if job.Status != JOB_STATUS_PROC {
                continue
            }
            runAt := job.RunAt
            if runAt < job.SchedAt {
                runAt = job.SchedAt
            }
            if runAt + job.Timeout < current {
                updateQueue = append(updateQueue, job)
            } else {
                sched.jobQueue.PushBack(job)
                sched.IncrStatProc(job)
            }
        }
    }

    for _, job := range updateQueue {
        job.Status = JOB_STATUS_READY
        sched.store.Save(job)
    }

    for _, job := range removeQueue {
        sched.store.Delete(job.Id)
    }
}


func (sched *Sched) Close() {
}
