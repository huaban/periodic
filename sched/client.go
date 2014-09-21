package sched

import (
    "io"
    "log"
    "bytes"
    "strconv"
    "encoding/json"
)


type Client struct {
    sched *Sched
    conn Conn
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
    var cmd Command
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

        msgId, cmd, payload = ParseCommand(payload)

        switch cmd {
        case SUBMIT_JOB:
            err = client.HandleSubmitJob(msgId, payload)
            break
        case STATUS:
            err = client.HandleStatus(msgId)
            break
        case PING:
            err = client.HandleCommand(msgId, PONG)
            break
        case DROP_FUNC:
            err = client.HandleDropFunc(msgId, payload)
            break
        default:
            err = client.HandleCommand(msgId, UNKNOWN)
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


func (client *Client) HandleCommand(msgId int64, cmd Command) (err error) {
    buf := bytes.NewBuffer(nil)
    buf.WriteString(strconv.FormatInt(msgId, 10))
    buf.Write(NULL_CHAR)
    buf.Write(cmd.Bytes())
    err = client.conn.Send(buf.Bytes())
    return
}


func (client *Client) HandleSubmitJob(msgId int64, payload []byte) (err error) {
    var job Job
    var e error
    var conn = client.conn
    var sched = client.sched
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    job, e = NewJob(payload)
    if e != nil {
        err = conn.Send([]byte(e.Error()))
        return
    }
    is_new := true
    changed := false
    job.Status = JOB_STATUS_READY
    oldJob, e := sched.driver.GetOne(job.Func, job.Name)
    if e == nil && oldJob.Id > 0 {
        job.Id = oldJob.Id
        if oldJob.Status == JOB_STATUS_PROC {
            sched.DecrStatProc(oldJob)
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
    sched.Notify()
    err = client.HandleCommand(msgId, SUCCESS)
    return
}


func (client *Client) HandleStatus(msgId int64) (err error) {
    data, _ := json.Marshal(client.sched.Funcs)
    buf := bytes.NewBuffer(nil)
    buf.WriteString(strconv.FormatInt(msgId, 10))
    buf.Write(NULL_CHAR)
    buf.Write(data)
    err = client.conn.Send(buf.Bytes())
    return
}


func (client *Client) HandleDropFunc(msgId int64, payload []byte) (err error) {
    Func := string(payload)
    stat, ok := client.sched.Funcs[Func]
    sched := client.sched
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    if ok && stat.Worker == 0 {
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
        delete(client.sched.Funcs, Func)
        delete(client.sched.jobPQ, Func)
    }
    err = client.HandleCommand(msgId, SUCCESS)
    return
}
