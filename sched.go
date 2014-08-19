package main

import (
    "log"
    "net"
    "time"
    "sync"
    "container/list"
    "huabot-sched/db"
)


type Sched struct {
    TotalWorkerCount int
    timer            *time.Timer
    grabQueue        *list.List
    jobQueue         *list.List
    sockFile         string
    JobLocker        *sync.Mutex
}


func NewSched(sockFile string) *Sched {
    sched = new(Sched)
    sched.TotalWorkerCount = 0
    sched.timer = time.NewTimer(1 * time.Hour)
    sched.grabQueue = list.New()
    sched.jobQueue = list.New()
    sched.sockFile = sockFile
    sched.JobLocker = new(sync.Mutex)
    return sched
}


func (sched *Sched) Serve() {
    sockCheck(sched.sockFile)
    sched.checkJobQueue()
    go sched.handle()
    listen, err := net.Listen("unix", sched.sockFile)
    if err != nil {
        log.Fatal(err)
    }
    defer listen.Close()
    log.Printf("huabot-sched started on %s\n", sched.sockFile)
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
    sched.TotalWorkerCount -= 1
    log.Printf("Total worker: %d\n", sched.TotalWorkerCount)
    sched.removeGrabQueue(worker)
    worker.Close()
}

func (sched *Sched) HandleConnection(conn net.Conn) {
    worker := NewWorker(sched, Conn{Conn: conn})
    sched.TotalWorkerCount += 1
    log.Printf("Total worker: %d\n", sched.TotalWorkerCount)
    go worker.Handle()
}


func (sched *Sched) Done(jobId int64) {
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    db.DelJob(jobId)
    return
}


func (sched *Sched) isDoJob(job db.Job) bool {
    now := time.Now()
    current := int64(now.Unix())
    ret := false
    for e := sched.jobQueue.Front(); e != nil; e = e.Next() {
        chk := e.Value.(db.Job)
        runAt := chk.RunAt
        if runAt < chk.SchedAt {
            runAt = chk.SchedAt
        }
        if chk.Timeout > 0 && runAt + chk.Timeout > current {
            newJob, _ := db.GetJob(chk.Id)
            if newJob.Status == "doing" {
                newJob.Status = "ready"
                newJob.Save()
            }
            sched.jobQueue.Remove(e)
            continue
        }
        if chk.Id == job.Id {
            old := e.Value.(db.Job)
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


func (sched *Sched) SubmitJob(worker *Worker, job db.Job) {
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    if job.Name == "" {
        job.Delete()
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
        sched.DieWorker(worker)
        return
    }
    now := time.Now()
    current := int64(now.Unix())
    job.Status = "doing"
    job.RunAt = current
    job.Save()
    sched.jobQueue.PushBack(job)
    sched.removeGrabQueue(worker)
}


func (sched *Sched) handle() {
    var current time.Time
    var timestamp int64
    for {
        for e := sched.grabQueue.Front(); e != nil; e = e.Next() {
            worker := e.Value.(*Worker)
            jobs, err := db.RangeSchedJob("ready", 0, 0)
            if err != nil || len(jobs) == 0 {
                sched.timer.Reset(time.Minute)
                current =<-sched.timer.C
                continue
            }
            timestamp = int64(time.Now().Unix())
            if jobs[0].SchedAt < timestamp {
                sched.SubmitJob(worker, jobs[0])
            } else {
                sched.timer.Reset(time.Second * time.Duration(jobs[0].SchedAt - timestamp))
                current =<-sched.timer.C
                timestamp = int64(current.Unix())
                if jobs[0].SchedAt <= timestamp {
                    sched.SubmitJob(worker, jobs[0])
                }
            }
        }
        if sched.grabQueue.Len() == 0 {
            sched.timer.Reset(time.Minute)
            current =<-sched.timer.C
        }
    }
}


func (sched *Sched) Fail(jobId int64) {
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    job, _ := db.GetJob(jobId)
    job.Status = "ready"
    job.Save()
    return
}


func (sched *Sched) SchedLater(jobId int64, delay int64) {
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    job, _ := db.GetJob(jobId)
    job.Status = "ready"
    var now = time.Now()
    job.SchedAt = int64(now.Unix()) + delay
    job.Save()
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
    start := 0
    limit := 20
    total, _ := db.CountSchedJob("doing")
    updateQueue := make([]db.Job, 0)
    removeQueue := make([]db.Job, 0)
    var now = time.Now()
    current := int64(now.Unix())

    for start = 0; start < int(total); start += limit {
        jobs, _ := db.RangeSchedJob("doing", start, start + limit)
        for _, job := range jobs {
            if job.Name == "" {
                removeQueue = append(removeQueue, job)
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
            }
        }
    }

    for _, job := range updateQueue {
        job.Status = "ready"
        job.Save()
    }

    for _, job := range removeQueue {
        job.Delete()
    }
}


func (sched *Sched) Close() {
}
