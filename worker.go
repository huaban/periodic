package main

import (
    "log"
    "container/list"
    "github.com/docker/libchan/unix"
    "github.com/docker/libchan/data"
    "huabot-sched/db"
    "strconv"
)

type Worker struct {
    jobs *list.List
    conn *unix.UnixConn
    sched *Sched
}


func NewWorker(sched *Sched, conn *unix.UnixConn) (worker *Worker) {
    worker = new(Worker)
    worker.conn = conn
    worker.jobs = list.New()
    worker.sched = sched
    return
}


func (worker *Worker) HandeNewConnection() {
    if err := worker.conn.Send(data.Empty().Set("type", "connection").Bytes(), nil); err != nil {
        worker.sched.die_worker <- worker
        log.Printf("Error: %s\n", err.Error())
        return
    }
    go worker.Handle()
}


func (worker *Worker) HandleDo(job db.Job) {
    worker.jobs.PushBack(job)
    pack, err := packJob(job)
    if err != nil {
        log.Printf("Error: %s\n", err.Error())
        return
    }
    if err := worker.conn.Send(pack, nil); err != nil {
        worker.sched.die_worker <- worker
        log.Printf("Error: %s\n", err.Error())
        return
    }
    job.Status = "doing"
    job.Save()
    go worker.Handle()
}


func (worker *Worker) HandleDone(jobId int) {
    worker.sched.Done(jobId)
    removeListJob(worker.jobs, jobId)
    go worker.Handle()
}


func (worker *Worker) HandleFail(jobId int) {
    worker.sched.Fail(jobId)
    removeListJob(worker.jobs, jobId)
    go worker.Handle()
}


func (worker *Worker) HandleWaitForJob() {
    if err := worker.conn.Send(data.Empty().Set("workload", "wait_for_job").Bytes(), nil); err != nil {
        worker.sched.die_worker <- worker
        log.Printf("Error: %s\n", err.Error())
        return
    }
    go worker.Handle()
}


func (worker *Worker) HandleSchedLater(jobId, delay int) {
    worker.sched.SchedLater(jobId, delay)
    removeListJob(worker.jobs, jobId)
    go worker.Handle()
}


func (worker *Worker) HandleNoJob() {
    if err := worker.conn.Send(data.Empty().Set("workload", "no_job").Bytes(), nil); err != nil {
        worker.sched.die_worker <- worker
        log.Printf("Error: %s\n", err.Error())
        return
    }
    go worker.Handle()
}


func (worker *Worker) Handle() {
    var payload []byte
    var err error
    var conn = worker.conn
    payload, _, err = conn.Receive()
    if err != nil {
        log.Printf("Error: %s\n", err.Error())
        worker.sched.die_worker <- worker
        return
    }
    msg := data.Message(string(payload));
    cmd := msg.Get("cmd")
    switch cmd[0] {
    case "ask":
        worker.sched.ask_worker <- worker
        break
    case "done":
        jobId, _ := strconv.Atoi(msg.Get("job_handle")[0])
        worker.HandleDone(jobId)
        break
    case "fail":
        jobId, _ := strconv.Atoi(msg.Get("job_handle")[0])
        worker.HandleFail(jobId)
        break
    case "sched_later":
        jobId, _ := strconv.Atoi(msg.Get("job_handle")[0])
        delay, _ := strconv.Atoi(msg.Get("delay")[0])
        worker.HandleSchedLater(jobId, delay)
        break
    case "sleep":
        if err = conn.Send(data.Empty().Set("workload", "nop").Bytes(), nil); err != nil {
            log.Printf("Error: %s\n", err.Error())
        }
        go worker.Handle()
        break
    case "ping":
        if err = conn.Send(data.Empty().Set("workload", "pong").Bytes(), nil); err != nil {
            log.Printf("Error: %s\n", err.Error())
        }
        go worker.Handle()
        break
    default:
        if err = conn.Send(data.Empty().Set("error", "command: " + cmd[0] + " unknown").Bytes(), nil); err != nil {
            log.Printf("Error: %s\n", err.Error())
            worker.sched.die_worker <- worker
        }
        go worker.Handle()
        break
    }
}


func (worker *Worker) Close() {
    worker.conn.Close()
    for e := worker.jobs.Front(); e != nil; e = e.Next() {
        worker.sched.Fail(e.Value.(db.Job).Id)
    }
}
