package sched

import (
    "log"
    "encoding/json"
    "container/heap"
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
    oldJob, e := sched.store.GetOne(job.Func, job.Name)
    if e == nil && oldJob.Id > 0 {
        job.Id = oldJob.Id
        if oldJob.Status == JOB_STATUS_PROC {
            sched.DecrStatProc(oldJob)
        }
        is_new = false
    }
    e = sched.store.Save(job)
    pq, ok := sched.pq[job.Func]
    if !ok {
        pq1 := make(PriorityQueue, 0)
        pq = &pq1
        sched.pq[job.Func] = pq
        heap.Init(pq)
    }
    item := &Item{
        value: job.Id,
        priority: job.SchedAt,
    }
    heap.Push(pq, item)
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
    if ok && stat.Worker == 0 {
        iter := sched.store.NewIterator(payload)
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
            sched.store.Delete(jobId)
        }
        delete(client.sched.Funcs, Func)
        delete(client.sched.pq, Func)
    }
    err = client.conn.Send([]byte("ok"))
    return
}
