package periodic

import (
    "io"
    "log"
    "bytes"
    "github.com/Lupino/periodic/driver"
    "github.com/Lupino/periodic/protocol"
)


type client struct {
    sched *Sched
    conn  protocol.Conn
}


func newClient(sched *Sched, conn protocol.Conn) (c *client) {
    c = new(client)
    c.conn = conn
    c.sched = sched
    return
}


func (c *client) handle() {
    var payload []byte
    var err error
    var msgId []byte
    var cmd protocol.Command
    var conn = c.conn
    defer func() {
        if x := recover(); x != nil {
            log.Printf("[client] painc: %v\n", x)
        }
    } ()
    defer conn.Close()
    for {
        payload, err = conn.Receive()
        if err != nil {
            if err != io.EOF {
                log.Printf("clientError: %s\n", err.Error())
            }
            return
        }

        msgId, cmd, payload = protocol.ParseCommand(payload)

        switch cmd {
        case protocol.SUBMIT_JOB:
            err = c.handleSubmitJob(msgId, payload)
            break
        case protocol.STATUS:
            err = c.handleStatus(msgId)
            break
        case protocol.PING:
            err = c.handleCommand(msgId, protocol.PONG)
            break
        case protocol.DROP_FUNC:
            err = c.handleDropFunc(msgId, payload)
            break
        default:
            err = c.handleCommand(msgId, protocol.UNKNOWN)
            break
        }
        if err != nil {
            if err != io.EOF {
                log.Printf("clientError: %s\n", err.Error())
            }
            return
        }
    }
}


func (c *client) handleCommand(msgId []byte, cmd protocol.Command) (err error) {
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(protocol.NULL_CHAR)
    buf.Write(cmd.Bytes())
    err = c.conn.Send(buf.Bytes())
    return
}


func (c *client) handleSubmitJob(msgId []byte, payload []byte) (err error) {
    var job driver.Job
    var e error
    var conn = c.conn
    var sched = c.sched
    defer sched.jobLocker.Unlock()
    sched.jobLocker.Lock()
    job, e = driver.NewJob(payload)
    if e != nil {
        err = conn.Send([]byte(e.Error()))
        return
    }
    is_new := true
    changed := false
    job.Status = driver.JOB_STATUS_READY
    oldJob, e := sched.driver.GetOne(job.Func, job.Name)
    if e == nil && oldJob.Id > 0 {
        job.Id = oldJob.Id
        if oldJob.Status == driver.JOB_STATUS_PROC {
            sched.decrStatProc(oldJob)
            sched.removeRevertPQ(job)
            changed = true
        }
        is_new = false
    }
    e = sched.driver.Save(&job)
    if e != nil {
        err = conn.Send([]byte(e.Error()))
        return
    }

    if is_new {
        sched.incrStatJob(job)
    }
    if is_new || changed {
        sched.pushJobPQ(job)
    }
    sched.notifyJobTimer()
    err = c.handleCommand(msgId, protocol.SUCCESS)
    return
}


func (c *client) handleStatus(msgId []byte) (err error) {
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(protocol.NULL_CHAR)
    for _, stat := range c.sched.stats {
        buf.WriteString(stat.String())
        buf.WriteString("\n")
    }
    err = c.conn.Send(buf.Bytes())
    return
}


func (c *client) handleDropFunc(msgId []byte, payload []byte) (err error) {
    Func := string(payload)
    stat, ok := c.sched.stats[Func]
    sched := c.sched
    defer sched.notifyJobTimer()
    defer sched.jobLocker.Unlock()
    sched.jobLocker.Lock()
    if ok && stat.Worker.Int() == 0 {
        iter := sched.driver.NewIterator(payload)
        deleteJob := make([]int64, 0)
        for {
            if !iter.Next() {
                break
            }
            job := iter.Value()
            deleteJob = append(deleteJob, job.Id)
        }
        iter.Close()
        for _, jobId := range deleteJob {
            sched.driver.Delete(jobId)
        }
        delete(c.sched.stats, Func)
        delete(c.sched.jobPQ, Func)
    }
    err = c.handleCommand(msgId, protocol.SUCCESS)
    return
}
