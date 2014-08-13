package main

import (
    "log"
    "container/list"
    "huabot-sched/db"
    "strconv"
    "bytes"
)


type Worker struct {
    jobs *list.List
    conn Conn
    sched *Sched
}


func NewWorker(sched *Sched, conn Conn) (worker *Worker) {
    worker = new(Worker)
    worker.conn = conn
    worker.jobs = list.New()
    worker.sched = sched
    return
}


func (worker *Worker) HandeNewConnection() {
    if err := worker.conn.Send([]byte("connection")); err != nil {
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
    if err := worker.conn.Send(pack); err != nil {
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
    if err := worker.conn.Send([]byte("wait_for_job")); err != nil {
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
    if err := worker.conn.Send([]byte("no_job")); err != nil {
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
    payload, err = conn.Receive()
    if err != nil {
        log.Printf("Error: %s\n", err.Error())
        worker.sched.die_worker <- worker
        return
    }

    buf := bytes.NewBuffer(nil)
    buf.WriteByte(NULL_CHAR)
    null_char := buf.Bytes()

    parts := bytes.SplitN(payload, null_char, 2)
    cmd := string(parts[0])
    switch cmd {
    case "ask":
        worker.sched.ask_worker <- worker
        break
    case "done":
        if len(parts) != 2 {
            log.Printf("Error: invalid format.")
            break
        }
        jobId, _ := strconv.Atoi(string(parts[1]))
        worker.HandleDone(jobId)
        break
    case "fail":
        if len(parts) != 2 {
            log.Printf("Error: invalid format.")
            break
        }
        jobId, _ := strconv.Atoi(string(parts[1]))
        worker.HandleFail(jobId)
        break
    case "sched_later":
        if len(parts) != 2 {
            log.Printf("Error: invalid format.")
            break
        }
        parts = bytes.SplitN(parts[1], null_char, 2)
        if len(parts) != 2 {
            log.Printf("Error: invalid format.")
            break
        }
        jobId, _ := strconv.Atoi(string(parts[0]))
        delay, _ := strconv.Atoi(string(parts[1]))
        worker.HandleSchedLater(jobId, delay)
        break
    case "sleep":
        if err := conn.Send([]byte("nop")); err != nil {
            log.Printf("Error: %s\n", err.Error())
        }
        go worker.Handle()
        break
    case "ping":
        if err := conn.Send([]byte("pong")); err != nil {
            log.Printf("Error: %s\n", err.Error())
        }
        go worker.Handle()
        break
    default:
        if err := conn.Send([]byte("unknown")); err != nil {
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
