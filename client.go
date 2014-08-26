package main

import (
    "log"
    "huabot-sched/db"
    "encoding/json"
)


const (
    SUBMIT_JOB = iota
    STATUS
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
    defer conn.Close()
    for {
        payload, err = conn.Receive()
        if err != nil {
            log.Printf("Error: %s\n", err.Error())
            return
        }

        switch payload[0] {
        case SUBMIT_JOB:
            err = client.HandleSubmitJob(payload[2:])
            break
        case STATUS:
            err = client.HandleStatus()
            break
        default:
            err = conn.Send(packCmd(UNKNOWN))
            break
        }
        if err != nil {
            log.Printf("Error: %s\n", err.Error())
            return
        }
    }
}


func (client *Client) HandleSubmitJob(payload []byte) (err error) {
    var job db.Job
    var e error
    var conn = client.conn
    var sched = client.sched
    e = json.Unmarshal(payload, &job)
    if e != nil {
        err = conn.Send([]byte(e.Error()))
        return
    }
    is_new := true
    jobId, _ := db.GetIndex("job:" + job.Func + ":name", job.Name)
    if jobId > 0 {
        job.Id = jobId
        if oldJob, e := db.GetJob(jobId); e == nil && oldJob.Status == db.JOB_STATUS_PROC {
            sched.DecrStatProc(oldJob)
        }
        is_new = false
    }
    e = job.Save()
    if e != nil {
        err = conn.Send([]byte(e.Error()))
        return
    }
    if is_new {
        sched.IncrStatJob(job)
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
