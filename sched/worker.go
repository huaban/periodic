package sched

import (
    "io"
    "log"
    "container/list"
    "strconv"
    "bytes"
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


func (worker *Worker) HandleDo(job Job) (err error){
    worker.jobQueue.PushBack(job)
    Pack, err := PackJob(job)
    if err != nil {
        log.Printf("Error: PackJob %d %s\n", job.Id, err.Error())
        return nil
    }
    err = worker.conn.Send(Pack)
    return
}


func (worker *Worker) HandleCanDo(Func string) error {
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
    worker.sched.Done(jobId)
    removeListJob(worker.jobQueue, jobId)
    return nil
}


func (worker *Worker) HandleFail(jobId int64) (err error) {
    worker.sched.Fail(jobId)
    removeListJob(worker.jobQueue, jobId)
    return nil
}


func (worker *Worker) HandleWaitForJob() (err error) {
    err = worker.conn.Send(WAIT_JOB.Bytes())
    return nil
}


func (worker *Worker) HandleSchedLater(jobId, delay int64) (err error){
    worker.sched.SchedLater(jobId, delay)
    removeListJob(worker.jobQueue, jobId)
    return nil
}


func (worker *Worker) HandleNoJob() (err error){
    err = worker.conn.Send(NO_JOB.Bytes())
    return
}


func (worker *Worker) HandleGrabJob() (err error){
    worker.sched.grabQueue.PushBack(worker)
    worker.sched.Notify()
    return nil
}


func (worker *Worker) Handle() {
    var payload []byte
    var err error
    var conn = worker.conn
    defer func() {
        if x := recover(); x != nil {
            log.Printf("[Worker] painc: %v\n", x)
        }
    } ()
    defer worker.Close()
    for {
        payload, err = conn.Receive()
        if err != nil {
            if err != io.EOF {
                log.Printf("WorkerError: %s\n", err.Error())
            }
            break
        }


        switch Command(payload[0]) {
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
            err = conn.Send(NOOP.Bytes())
            break
        case PING:
            err = conn.Send(PONG.Bytes())
            break
        case CAN_DO:
            err = worker.HandleCanDo(string(payload[2:]))
            break
        case CANT_DO:
            err = worker.HandleCanNoDo(string(payload[2:]))
            break
        default:
            err = conn.Send(UNKNOWN.Bytes())
            break
        }
        if err != nil {
            if err != io.EOF {
                log.Printf("WorkerError: %s\n", err.Error())
            }
            break
        }

        if !worker.alive {
            break
        }
    }
}


func (worker *Worker) Close() {
    defer worker.sched.Notify()
    defer worker.conn.Close()
    worker.sched.removeGrabQueue(worker)
    worker.alive = false
    for e := worker.jobQueue.Front(); e != nil; e = e.Next() {
        worker.sched.Fail(e.Value.(Job).Id)
    }
    for _, Func := range worker.Funcs {
        worker.sched.DecrStatFunc(Func)
    }
}
