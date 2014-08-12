package main

import (
    "log"
    "net"
    "time"
    "container/list"
    "huabot-sched/db"
    "github.com/docker/libchan/unix"
)


type Sched struct {
    new_worker chan *Worker
    ask_worker chan *Worker
    die_worker chan *Worker
    started bool
    worker_count int
    timer *time.Timer
    queue *list.List
    jobQueue *list.List
    sockFile string
}


func NewSched(sockFile string) *Sched {
    sched = new(Sched)
    sched.started = false
    sched.new_worker = make(chan *Worker, 1)
    sched.ask_worker = make(chan *Worker, 1)
    sched.die_worker = make(chan *Worker, 1)
    sched.worker_count = 0
    sched.timer = time.NewTimer(1 * time.Hour)
    sched.queue = list.New()
    sched.jobQueue = list.New()
    sched.sockFile = sockFile
    return sched
}


func (sched *Sched) Serve() {
    sched.started = true
    sockCheck(sched.sockFile)
    go sched.run()
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
        sched.NewConnectioin(conn)
    }
}


func (sched *Sched) Notify() {
    sched.timer.Reset(time.Millisecond)
}


func (sched *Sched) run() {
    var worker *Worker
    for {
        select {
        case worker = <-sched.new_worker:
            sched.worker_count += 1
            log.Printf("worker_count: %d\n", sched.worker_count)
            go worker.HandeNewConnection()
            break
        case worker =<-sched.ask_worker:
            sched.queue.PushBack(worker)
            sched.Notify()
            break
        case worker =<-sched.die_worker:
            sched.worker_count -= 1
            log.Printf("worker_count: %d\n", sched.worker_count)
            sched.removeQueue(worker)
            sched.Notify()
            worker.Close()
            break
        }
    }
    sched.started = false
}

func (sched *Sched) NewConnectioin(conn net.Conn) {
    uconn := conn.(*net.UnixConn)
    worker := NewWorker(sched, &unix.UnixConn{UnixConn: uconn})
    sched.new_worker <- worker
}


func (sched *Sched) Done(jobId int) {
    removeListJob(sched.jobQueue, jobId)
    return
}


func (sched *Sched) isDoJob(job db.Job) bool {
    for e := sched.jobQueue.Front(); e != nil; e = e.Next() {
        if e.Value.(db.Job).Id == job.Id {
            old := e.Value.(db.Job)
            now := time.Now()
            if old.SchedAt + old.Timeout < int(now.Unix()) {
                return true
            }
        }
    }
    return false
}


func (sched *Sched) SubmitJob(worker *Worker, job db.Job) {
    job.Status = "doing"
    job.Save()
    if sched.isDoJob(job) {
        return
    }
    sched.removeQueue(worker)
    go worker.HandleDo(job)
}


func (sched *Sched) handle() {
    var current time.Time
    var timestamp int
    for {
        for e := sched.queue.Front(); e != nil; e = e.Next() {
            worker := e.Value.(*Worker)
            jobs, err := db.RangeSchedJob("ready", 0, 0)
            if err != nil {
                sched.queue.Remove(e)
                go worker.HandleNoJob()
            }
            if len(jobs) == 0 {
                sched.timer.Reset(time.Minute)
                current =<-sched.timer.C
                continue
            }
            timestamp = int(time.Now().Unix())
            if jobs[0].SchedAt < timestamp {
                sched.SubmitJob(worker, jobs[0])
            } else {
                sched.timer.Reset(time.Second * time.Duration(jobs[0].SchedAt - timestamp))
                current =<-sched.timer.C
                timestamp = int(current.Unix())
                if jobs[0].SchedAt <= timestamp {
                    sched.SubmitJob(worker, jobs[0])
                }
            }
        }
        if sched.queue.Len() == 0 {
            current =<-sched.timer.C
        }
    }
}


func (sched *Sched) Fail(jobId int) {
    removeListJob(sched.jobQueue, jobId)
    job, _ := db.GetJob(jobId)
    job.Status = "ready"
    job.Save()
    return
}


func (sched *Sched) SchedLater(jobId int, delay int) {
    removeListJob(sched.jobQueue, jobId)
    job, _ := db.GetJob(jobId)
    job.Status = "ready"
    var now = time.Now()
    job.SchedAt = int(now.Unix()) + delay
    job.Save()
    return
}


func (sched *Sched) removeQueue(worker *Worker) {
    for e := sched.queue.Front(); e != nil; e = e.Next() {
        if e.Value.(*Worker) == worker {
            sched.queue.Remove(e)
        }
    }
}


func (sched *Sched) Close() {
}
