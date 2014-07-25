package main

import (
    "log"
    "net"
    "time"
    "github.com/docker/libchan/unix"
)


type Sched struct {
    new_worker chan *Worker
    ask_worker chan *Worker
    die_worker chan *Worker
    started bool
    worker_count int
    timer *time.Timer
}


func NewSched() *Sched {
    sched = new(Sched)
    sched.started = false
    sched.new_worker = make(chan *Worker, 1)
    sched.ask_worker = make(chan *Worker, 1)
    sched.die_worker = make(chan *Worker, 1)
    sched.worker_count = 0
    sched.timer = time.NewTimer(1 * time.Hour)
    return sched
}


func (sched *Sched) Start() {
    sched.started = true
    go sched.run()
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
            job, err := NextSchedJob(0, 0)
            if err != nil {
                go worker.HandleNoJob()
            }
            now := int(time.Now().Unix())
            if job[0].SchedAt < now {
                sched.SubmitJob(worker, job[0].JobId)
            } else {
                sched.timer.Reset(time.Second * time.Duration(job[0].SchedAt - now))
                <-sched.timer.C
                now := int(time.Now().Unix())
                if job[0].SchedAt <= now {
                    sched.SubmitJob(worker, job[0].JobId)
                } else {
                    go worker.HandleWaitForJob()
                }
            }
            break
        case worker =<-sched.die_worker:
            log.Printf("%v close\n", worker)
            sched.worker_count -= 1
            log.Printf("worker_count: %d\n", sched.worker_count)
            worker.Close()
            break
        case <-sched.timer.C:
            go worker.HandleDo("haha")
        }
    }
    sched.started = false
}

func (sched *Sched) NewConnectioin(conn net.Conn) {
    uconn := conn.(*net.UnixConn)
    worker := NewWorker(sched, &unix.UnixConn{UnixConn: uconn})
    sched.new_worker <- worker
    sched.Notify()
}


func (sched *Sched) Done(job string) {
    return
}


func (sched *Sched) SubmitJob(worker *Worker, job string) {
    delay := RandomDelay()
    SchedLater(job, delay)
    go worker.HandleDo(job)
}


func (sched *Sched) Fail(job string) {
    return
}


func (sched *Sched) Close() {
}
