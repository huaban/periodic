package periodic

import (
    "io"
    "log"
    "container/list"
    "strconv"
    "bytes"
    "github.com/Lupino/periodic/driver"
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


func (worker *Worker) HandleDo(msgId int64, job driver.Job) (err error){
    worker.jobQueue.PushBack(job)
    buf := bytes.NewBuffer(nil)
    buf.WriteString(strconv.FormatInt(msgId, 10))
    buf.Write(NULL_CHAR)
    buf.WriteString(strconv.FormatInt(job.Id, 10))
    buf.Write(NULL_CHAR)
    buf.Write(job.Bytes())
    err = worker.conn.Send(buf.Bytes())
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


func (worker *Worker) HandleCommand(msgId int64, cmd Command) (err error) {
    buf := bytes.NewBuffer(nil)
    buf.WriteString(strconv.FormatInt(msgId, 10))
    buf.Write(NULL_CHAR)
    buf.Write(cmd.Bytes())
    err = worker.conn.Send(buf.Bytes())
    return
}


func (worker *Worker) HandleSchedLater(jobId, delay int64) (err error){
    worker.sched.SchedLater(jobId, delay)
    removeListJob(worker.jobQueue, jobId)
    return nil
}


func (worker *Worker) HandleGrabJob(msgId int64) (err error){
    item := GrabItem{
        w: worker,
        msgId: msgId,
    }
    worker.sched.grabQueue.Push(item)
    worker.sched.NotifyJobTimer()
    return nil
}


func (worker *Worker) Handle() {
    var payload []byte
    var err error
    var conn = worker.conn
    var msgId int64
    var cmd Command
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

        msgId, cmd, payload = ParseCommand(payload)

        switch cmd {
        case GRAB_JOB:
            err = worker.HandleGrabJob(msgId)
            break
        case JOB_DONE:
            jobId, _ := strconv.ParseInt(string(payload), 10, 0)
            err = worker.HandleDone(jobId)
            break
        case JOB_FAIL:
            jobId, _ := strconv.ParseInt(string(payload), 10, 0)
            err = worker.HandleFail(jobId)
            break
        case SCHED_LATER:
            parts := bytes.SplitN(payload, NULL_CHAR, 2)
            if len(parts) != 2 {
                log.Printf("Error: invalid format.")
                break
            }
            jobId, _ := strconv.ParseInt(string(parts[0]), 10, 0)
            delay, _ := strconv.ParseInt(string(parts[1]), 10, 0)
            err = worker.HandleSchedLater(jobId, delay)
            break
        case SLEEP:
            err = worker.HandleCommand(msgId, NOOP)
            break
        case PING:
            err = worker.HandleCommand(msgId, PONG)
            break
        case CAN_DO:
            err = worker.HandleCanDo(string(payload))
            break
        case CANT_DO:
            err = worker.HandleCanNoDo(string(payload))
            break
        default:
            err = worker.HandleCommand(msgId, UNKNOWN)
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
    defer worker.sched.NotifyJobTimer()
    defer worker.conn.Close()
    worker.sched.grabQueue.RemoveWorker(worker)
    worker.alive = false
    for e := worker.jobQueue.Front(); e != nil; e = e.Next() {
        worker.sched.Fail(e.Value.(driver.Job).Id)
    }
    for _, Func := range worker.Funcs {
        worker.sched.DecrStatFunc(Func)
    }
}
