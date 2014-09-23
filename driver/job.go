package driver

import (
    "encoding/json"
)


const (
    JOB_STATUS_READY = "ready"
    JOB_STATUS_PROC  = "processing"
)


type Job struct {
    Id      int64  `json:"job_id"`
    Name    string `json:"name"`
    Func    string `json:"func"`
    Args    string `json:"workload"`
    Timeout int64  `json:"timeout"`
    SchedAt int64  `json:"sched_at"`
    RunAt   int64  `json:"run_at"`
    Status  string `json:"status"`
}


func NewJob(payload []byte) (job Job, err error) {
    err = json.Unmarshal(payload, &job)
    return
}


func (job Job) Bytes() (data []byte) {
    data, _ = json.Marshal(job)
    return
}
