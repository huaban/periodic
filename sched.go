package main

import (
    "log"
    "net"
    "github.com/docker/libchan/unix"
)


type Sched struct {
    new_worker chan *Worker
    ask_worker chan *Worker
    die_worker chan *Worker
    started bool
    worker_count int
}


func NewSched() *Sched {
    sched = new(Sched)
    sched.started = false
    sched.new_worker = make(chan *Worker, 1)
    sched.ask_worker = make(chan *Worker, 1)
    sched.die_worker = make(chan *Worker, 1)
    sched.worker_count = 0
    return sched
}


func (sched *Sched) Start() {
    sched.started = true
    go sched.run()
}


func (sched *Sched) Notify() {
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
            go worker.HandleDo("haha")
            break
        case worker =<-sched.die_worker:
            log.Printf("%v close\n", worker)
            sched.worker_count -= 1
            log.Printf("worker_count: %d\n", sched.worker_count)
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


func (sched *Sched) Fail(job string) {
    return
}


func (sched *Sched) Close() {
}
