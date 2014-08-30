package sched


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


const (
    JOB_STATUS_READY = "ready"
    JOB_STATUS_PROC  = "doing"
)


type Storer interface {
    Save(Job) error
    Delete(jobId int64) error
    Get(jobId int64) (Job, error)
    Count() (int64, error)
    Next(Func string) (Job, error)
    GetOneByFunc(Func string, jobName string) (Job, error)
    GetAll(start int64, stop int64) ([]Job, error)
    GetAllByFunc(Func string, jobStatus string, start int64, stop int64) ([]Job, error)
}
