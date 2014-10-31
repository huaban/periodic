package periodic

import (
    "io"
    "log"
    "bytes"
    "strconv"
    "github.com/Lupino/periodic/driver"
    "github.com/Lupino/periodic/protocol"
)


type Client struct {
    sched *Sched
    conn  Conn
}


func NewClient(sched *Sched, conn Conn) (client *Client) {
    client = new(Client)
    client.conn = conn
    client.sched = sched
    return
}


func (client *Client) Handle() {
    var payload []byte
    var err error
    var msgId int64
    var cmd protocol.Command
    var conn = client.conn
    defer func() {
        if x := recover(); x != nil {
            log.Printf("[Client] painc: %v\n", x)
        }
    } ()
    defer conn.Close()
    for {
        payload, err = conn.Receive()
        if err != nil {
            if err != io.EOF {
                log.Printf("ClientError: %s\n", err.Error())
            }
            return
        }

        msgId, cmd, payload = protocol.ParseCommand(payload)

        switch cmd {
        case protocol.SUBMIT_JOB:
            err = client.HandleSubmitJob(msgId, payload)
            break
        case protocol.STATUS:
            err = client.HandleStatus(msgId)
            break
        case protocol.PING:
            err = client.HandleCommand(msgId, protocol.PONG)
            break
        case protocol.DROP_FUNC:
            err = client.HandleDropFunc(msgId, payload)
            break
        default:
            err = client.HandleCommand(msgId, protocol.UNKNOWN)
            break
        }
        if err != nil {
            if err != io.EOF {
                log.Printf("ClientError: %s\n", err.Error())
            }
            return
        }
    }
}


func (client *Client) HandleCommand(msgId int64, cmd protocol.Command) (err error) {
    buf := bytes.NewBuffer(nil)
    buf.WriteString(strconv.FormatInt(msgId, 10))
    buf.Write(protocol.NULL_CHAR)
    buf.Write(cmd.Bytes())
    err = client.conn.Send(buf.Bytes())
    return
}


func (client *Client) HandleSubmitJob(msgId int64, payload []byte) (err error) {
    var job driver.Job
    var e error
    var conn = client.conn
    var sched = client.sched
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
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
            sched.DecrStatProc(oldJob)
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
        sched.IncrStatJob(job)
    }
    if is_new || changed {
        sched.pushJobPQ(job)
    }
    sched.NotifyJobTimer()
    err = client.HandleCommand(msgId, protocol.SUCCESS)
    return
}


func (client *Client) HandleStatus(msgId int64) (err error) {
    buf := bytes.NewBuffer(nil)
    buf.WriteString(strconv.FormatInt(msgId, 10))
    buf.Write(protocol.NULL_CHAR)
    for _, stat := range client.sched.stats {
        buf.WriteString(stat.String())
        buf.WriteString("\n")
    }
    err = client.conn.Send(buf.Bytes())
    return
}


func (client *Client) HandleDropFunc(msgId int64, payload []byte) (err error) {
    Func := string(payload)
    stat, ok := client.sched.stats[Func]
    sched := client.sched
    defer sched.NotifyJobTimer()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
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
        delete(client.sched.stats, Func)
        delete(client.sched.jobPQ, Func)
    }
    err = client.HandleCommand(msgId, protocol.SUCCESS)
    return
}
