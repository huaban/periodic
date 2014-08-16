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
    alive bool
}


func NewWorker(sched *Sched, conn Conn) (worker *Worker) {
    worker = new(Worker)
    worker.conn = conn
    worker.jobs = list.New()
    worker.sched = sched
    worker.alive = true
    return
}


func (worker *Worker) HandleDo(job db.Job) (err error){
    log.Printf("HandleDo: %d\n", job.Id)
    worker.jobs.PushBack(job)
    pack, err := packJob(job)
    if err != nil {
        log.Printf("Error: packJob %d %s\n", job.Id, err.Error())
        return nil
    }
    err = worker.conn.Send(pack)
    if err != nil {
        return err
    }
    job.Status = "doing"
    job.Save()
    return nil
}


func (worker *Worker) HandleDone(jobId int) (err error) {
    log.Printf("HandleDone: %d\n", jobId)
    worker.sched.Done(jobId)
    removeListJob(worker.jobs, jobId)
    return nil
}


func (worker *Worker) HandleFail(jobId int) (err error) {
    log.Printf("HandleFail: %d\n", jobId)
    worker.sched.Fail(jobId)
    removeListJob(worker.jobs, jobId)
    return nil
}


func (worker *Worker) HandleWaitForJob() (err error) {
    log.Printf("HandleWaitForJob\n")
    err = worker.conn.Send([]byte("wait_for_job"))
    return nil
}


func (worker *Worker) HandleSchedLater(jobId, delay int) (err error){
    log.Printf("HandleSchedLater: %d %d\n", jobId, delay)
    worker.sched.SchedLater(jobId, delay)
    removeListJob(worker.jobs, jobId)
    return nil
}


func (worker *Worker) HandleNoJob() (err error){
    log.Printf("HandleNoJob\n")
    err = worker.conn.Send([]byte("no_job"))
    return
}


func (worker *Worker) HandleGrabJob() (err error){
    log.Printf("HandleGrabJob\n")
    worker.sched.grabQueue.PushBack(worker)
    worker.sched.Notify()
    return nil
}


func (worker *Worker) Handle() {
    var payload []byte
    var err error
    var conn = worker.conn
    for {
        payload, err = conn.Receive()
        if err != nil {
            log.Printf("Error: %s\n", err.Error())
            worker.sched.DieWorker(worker)
            return
        }

        buf := bytes.NewBuffer(nil)
        buf.WriteByte(NULL_CHAR)
        null_char := buf.Bytes()

        parts := bytes.SplitN(payload, null_char, 2)
        cmd := string(parts[0])
        switch cmd {
        case "grab":
            err = worker.HandleGrabJob()
            break
        case "done":
            if len(parts) != 2 {
                log.Printf("Error: invalid format.")
                break
            }
            jobId, _ := strconv.Atoi(string(parts[1]))
            err = worker.HandleDone(jobId)
            break
        case "fail":
            if len(parts) != 2 {
                log.Printf("Error: invalid format.")
                break
            }
            jobId, _ := strconv.Atoi(string(parts[1]))
            err = worker.HandleFail(jobId)
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
            err = worker.HandleSchedLater(jobId, delay)
            break
        case "sleep":
            err = conn.Send([]byte("nop"))
            break
        case "ping":
            err = conn.Send([]byte("pong"))
            break
        default:
            err = conn.Send([]byte("unknown"))
            break
        }
        if err != nil {
            log.Printf("Error: %s\n", err.Error())
            worker.alive = false
            worker.sched.DieWorker(worker)
            return
        }

        if !worker.alive {
            break
        }
    }
}


func (worker *Worker) Close() {
    worker.conn.Close()
    for e := worker.jobs.Front(); e != nil; e = e.Next() {
        worker.sched.Fail(e.Value.(db.Job).Id)
    }
}
