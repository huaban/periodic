package sched

import (
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
        case PING:
            err = conn.Send(packCmd(PONG))
            break
        case DROP_FUNC:
            err = client.HandleDropFunc(payload[2:])
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
    var job Job
    var e error
    var conn = client.conn
    var sched = client.sched
    e = json.Unmarshal(payload, &job)
    if e != nil {
        err = conn.Send([]byte(e.Error()))
        return
    }
    is_new := true
    job.Status = JOB_STATUS_READY
    oldJob, e := sched.store.GetOneByFunc(job.Func, job.Name)
    if e == nil && oldJob.Id > 0 {
        job.Id = oldJob.Id
        if oldJob.Status == JOB_STATUS_PROC {
            sched.DecrStatProc(oldJob)
        }
        is_new = false
    }
    e = sched.store.Save(job)
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


func (client *Client) HandleDropFunc(payload []byte) (err error) {
    Func := string(payload)
    stat, ok := client.sched.Funcs[Func]
    sched := client.sched
    if ok && stat.TotalWorker == 0 {
        if jobs, e := sched.store.GetAllByFunc(Func, JOB_STATUS_READY, 0, -1); e == nil {
            for _, job := range jobs {
                sched.DecrStatJob(job)
                sched.store.Delete(job.Id)
            }
        }

        if jobs, e := sched.store.GetAllByFunc(Func, JOB_STATUS_PROC, 0, -1); e == nil {
            for _, job := range jobs {
                sched.DecrStatJob(job)
                sched.DecrStatProc(job)
                sched.store.Delete(job.Id)
            }
        }
    }
    err = client.conn.Send([]byte("ok"))
    return
}
