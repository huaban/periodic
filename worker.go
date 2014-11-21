package periodic

import (
    "io"
    "log"
    "sync"
    "strconv"
    "bytes"
    "github.com/Lupino/periodic/driver"
    "github.com/Lupino/periodic/protocol"
)


type Worker struct {
    jobQueue map[int64]driver.Job
    conn     protocol.Conn
    sched    *Sched
    alive    bool
    Funcs    []string
    locker   *sync.Mutex
}


func NewWorker(sched *Sched, conn protocol.Conn) (worker *Worker) {
    worker = new(Worker)
    worker.conn = conn
    worker.jobQueue = make(map[int64]driver.Job)
    worker.sched = sched
    worker.Funcs = make([]string, 0)
    worker.alive = true
    worker.locker = new(sync.Mutex)
    return
}


func (worker *Worker) IsAlive() bool {
    return worker.alive
}


func (worker *Worker) HandleDo(msgId []byte, job driver.Job) (err error){
    defer worker.locker.Unlock()
    worker.locker.Lock()
    worker.jobQueue[job.Id] = job
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(protocol.NULL_CHAR)
    buf.WriteString(strconv.FormatInt(job.Id, 10))
    buf.Write(protocol.NULL_CHAR)
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
    defer worker.locker.Unlock()
    worker.locker.Lock()
    if _, ok := worker.jobQueue[jobId]; ok {
        delete(worker.jobQueue, jobId)
    }
    return nil
}


func (worker *Worker) HandleFail(jobId int64) (err error) {
    worker.sched.Fail(jobId)
    defer worker.locker.Unlock()
    worker.locker.Lock()
    if _, ok := worker.jobQueue[jobId]; ok {
        delete(worker.jobQueue, jobId)
    }
    return nil
}


func (worker *Worker) HandleCommand(msgId []byte, cmd protocol.Command) (err error) {
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(protocol.NULL_CHAR)
    buf.Write(cmd.Bytes())
    err = worker.conn.Send(buf.Bytes())
    return
}


func (worker *Worker) HandleSchedLater(jobId, delay int64) (err error){
    worker.sched.SchedLater(jobId, delay)
    defer worker.locker.Unlock()
    worker.locker.Lock()
    if _, ok := worker.jobQueue[jobId]; ok {
        delete(worker.jobQueue, jobId)
    }
    return nil
}


func (worker *Worker) HandleGrabJob(msgId []byte) (err error){
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
    var msgId []byte
    var cmd protocol.Command
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

        msgId, cmd, payload = protocol.ParseCommand(payload)

        switch cmd {
        case protocol.GRAB_JOB:
            err = worker.HandleGrabJob(msgId)
            break
        case protocol.JOB_DONE:
            jobId, _ := strconv.ParseInt(string(payload), 10, 0)
            err = worker.HandleDone(jobId)
            break
        case protocol.JOB_FAIL:
            jobId, _ := strconv.ParseInt(string(payload), 10, 0)
            err = worker.HandleFail(jobId)
            break
        case protocol.SCHED_LATER:
            parts := bytes.SplitN(payload, protocol.NULL_CHAR, 2)
            if len(parts) != 2 {
                log.Printf("Error: invalid format.")
                break
            }
            jobId, _ := strconv.ParseInt(string(parts[0]), 10, 0)
            delay, _ := strconv.ParseInt(string(parts[1]), 10, 0)
            err = worker.HandleSchedLater(jobId, delay)
            break
        case protocol.SLEEP:
            err = worker.HandleCommand(msgId, protocol.NOOP)
            break
        case protocol.PING:
            err = worker.HandleCommand(msgId, protocol.PONG)
            break
        case protocol.CAN_DO:
            err = worker.HandleCanDo(string(payload))
            break
        case protocol.CANT_DO:
            err = worker.HandleCanNoDo(string(payload))
            break
        default:
            err = worker.HandleCommand(msgId, protocol.UNKNOWN)
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
    for k, _ := range worker.jobQueue {
        worker.sched.Fail(k)
    }
    worker.jobQueue = nil
    for _, Func := range worker.Funcs {
        worker.sched.DecrStatFunc(Func)
    }
    worker = nil
}
