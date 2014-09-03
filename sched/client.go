package sched

import (
    "io"
    "log"
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

        switch payload[0] {
        case SUBMIT_JOB:
            err = client.HandleSubmitJob(payload[2:])
            break
        case STATUS:
            err = client.HandleStatus()
            break
        case PING:
            err = conn.Send(PackCmd(PONG))
            break
        case DROP_FUNC:
            err = client.HandleDropFunc(payload[2:])
            break
        default:
            err = conn.Send(PackCmd(UNKNOWN))
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


func (client *Client) HandleSubmitJob(payload []byte) (err error) {
    var job Job
    var e error
    var conn = client.conn
    var sched = client.sched
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    e = json.Unmarshal(payload, &job)
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
    err = conn.Send([]byte("ok"))
    return
}


func (client *Client) HandleStatus() (err error) {
    var conn = client.conn
    var sched = client.sched
    data, _ := json.Marshal(sched.Funcs)
    err = conn.Send(data)
    return
}


func (client *Client) HandleDropFunc(payload []byte) (err error) {
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
    err = client.conn.Send([]byte("ok"))
    return
}
