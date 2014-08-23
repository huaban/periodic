package main

import (
    "log"
    "container/list"
    "huabot-sched/db"
    "strconv"
    "bytes"
)


const (
    NOOP = iota // server
    // for job
    GRAB_JOB    // client
    SCHED_LATER // client
    JOB_DONE    // client
    JOB_FAIL    // client
    WAIT_JOB    // server
    NO_JOB      // server
    // for func
    CAN_DO      // client
    CANT_DO     // client
    // for test
    PING        // client
    PONG        // server
    // other
    SLEEP       // client
    UNKNOWN     // server
)


type Worker struct {
    jobQueue *list.List
    conn     Conn
    sched    *Sched
    alive    bool
    Funcs    []string
}


func NewWorker(sched *Sched, conn Conn) (worker *Worker) {
    worker = new(Worker)
    worker.conn = conn
    worker.jobQueue = list.New()
    worker.sched = sched
    worker.Funcs = make([]string, 0)
    worker.alive = true
    return
}


func (worker *Worker) IsAlive() bool {
    return worker.alive
}


func (worker *Worker) HandleDo(job db.Job) (err error){
    log.Printf("HandleDo: %d\n", job.Id)
    worker.jobQueue.PushBack(job)
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


func (worker *Worker) HandleCanDo(Func string) error {
    log.Printf("HandleCanDo: %s\n", Func)
    for _, f := range worker.Funcs {
        if f == Func {
            return nil
        }
    }
    worker.Funcs = append(worker.Funcs, Func)
    worker.sched.IncrStatFunc(Func)
    return nil
}


func (worker *Worker) HandleCanNoDo(Func string) error {
    log.Printf("HandleCanDo: %s\n", Func)
    newFuncs := make([]string, 0)
    for _, f := range worker.Funcs {
        if f == Func {
            continue
        }
        newFuncs = append(newFuncs, f)
    }
    worker.Funcs = newFuncs
    return nil
}


func (worker *Worker) HandleDone(jobId int64) (err error) {
    log.Printf("HandleDone: %d\n", jobId)
    worker.sched.Done(jobId)
    removeListJob(worker.jobQueue, jobId)
    return nil
}


func (worker *Worker) HandleFail(jobId int64) (err error) {
    log.Printf("HandleFail: %d\n", jobId)
    worker.sched.Fail(jobId)
    removeListJob(worker.jobQueue, jobId)
    return nil
}


func (worker *Worker) HandleWaitForJob() (err error) {
    log.Printf("HandleWaitForJob\n")
    err = worker.conn.Send(packCmd(WAIT_JOB))
    return nil
}


func (worker *Worker) HandleSchedLater(jobId, delay int64) (err error){
    log.Printf("HandleSchedLater: %d %d\n", jobId, delay)
    worker.sched.SchedLater(jobId, delay)
    removeListJob(worker.jobQueue, jobId)
    return nil
}


func (worker *Worker) HandleNoJob() (err error){
    log.Printf("HandleNoJob\n")
    err = worker.conn.Send(packCmd(NO_JOB))
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


        switch payload[0] {
        case GRAB_JOB:
            err = worker.HandleGrabJob()
            break
        case JOB_DONE:
            jobId, _ := strconv.ParseInt(string(payload[2:]), 10, 0)
            err = worker.HandleDone(jobId)
            break
        case JOB_FAIL:
            jobId, _ := strconv.ParseInt(string(payload[2:]), 10, 0)
            err = worker.HandleFail(jobId)
            break
        case SCHED_LATER:
            parts := bytes.SplitN(payload[2:], NULL_CHAR, 2)
            if len(parts) != 2 {
                log.Printf("Error: invalid format.")
                break
            }
            jobId, _ := strconv.ParseInt(string(parts[0]), 10, 0)
            delay, _ := strconv.ParseInt(string(parts[1]), 10, 0)
            err = worker.HandleSchedLater(jobId, delay)
            break
        case SLEEP:
            err = conn.Send(packCmd(NOOP))
            break
        case PING:
            err = conn.Send(packCmd(PONG))
            break
        case CAN_DO:
            err = worker.HandleCanDo(string(payload[2:]))
            break
        case CANT_DO:
            err = worker.HandleCanNoDo(string(payload[2:]))
            break
        default:
            err = conn.Send(packCmd(UNKNOWN))
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
    for e := worker.jobQueue.Front(); e != nil; e = e.Next() {
        worker.sched.Fail(e.Value.(db.Job).Id)
    }
}
