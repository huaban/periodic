package main

import (
    "log"
    "net"
    "time"
    "container/list"
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
}


func NewSched() *Sched {
    sched = new(Sched)
    sched.started = false
    sched.new_worker = make(chan *Worker, 1)
    sched.ask_worker = make(chan *Worker, 1)
    sched.die_worker = make(chan *Worker, 1)
    sched.worker_count = 0
    sched.timer = time.NewTimer(1 * time.Hour)
    sched.queue = list.New()
    return sched
}


func (sched *Sched) Start() {
    sched.started = true
    go sched.run()
    go sched.handle()
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
            log.Printf("%v close\n", worker)
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


func (sched *Sched) Done(job string) {
    return
}


func (sched *Sched) SubmitJob(worker *Worker, job string) {
    delay := RandomDelay()
    SchedLater(job, delay)
    go worker.HandleDo(job)
}


func (sched *Sched) handle() {
    var current time.Time
    var timestamp int
    for {
        for e := sched.queue.Front(); e != nil; e = e.Next() {
            worker := e.Value.(*Worker)
            job, err := NextSchedJob(0, 0)
            if err != nil {
                sched.queue.Remove(e)
                go worker.HandleNoJob()
            }
            timestamp = int(time.Now().Unix())
            if job[0].SchedAt < timestamp {
                sched.queue.Remove(e)
                sched.SubmitJob(worker, job[0].JobId)
            } else {
                sched.timer.Reset(time.Second * time.Duration(job[0].SchedAt - timestamp))
                current =<-sched.timer.C
                timestamp = int(current.Unix())
                if job[0].SchedAt <= timestamp {
                    sched.queue.Remove(e)
                    sched.SubmitJob(worker, job[0].JobId)
                }
            }
        }
        if sched.queue.Len() == 0 {
            current =<-sched.timer.C
        }
    }
}


func (sched *Sched) Fail(job string) {
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
