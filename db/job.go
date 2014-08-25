package db


import (
    "log"
    "fmt"
    "errors"
    "strconv"
)

const (
    JOB_STATUS_READY = "ready"
    JOB_STATUS_PROC  = "doing"
)

// func name  unique key
// func status sched_at
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


func (job *Job) Save() (err error) {
    var tableName = GetTableName(*job)
    var key string
    var prefix = tableName + ":" + job.Func + ":"
    if job.Id > 0 {
        var old Job
        key = tableName + ":" + strconv.FormatInt(job.Id, 10)
        err = GetObject(key, &old)
        if err != nil || old.Id < 1 {
            err = errors.New(fmt.Sprintf("Update Job %d fail, the old job is not exists.", job.Id))
            return
        }
        if old.Name != job.Name {
            if e := DelIndex(prefix + "name", old.Name); e != nil {
                log.Printf("DelIndex Error: %s %s\n", prefix + "name", old.Name)
            }
        }
        if old.Status != job.Status {
            if e := DelIndex(prefix + old.Status + ":sched", strconv.FormatInt(job.Id, 10)); e != nil {
                log.Printf("DelIndex Error: %s %d\n", prefix + old.Status + ":sched", old.Id)
            }
        }
    } else {
        job.Id, err = NextSequence(tableName)
        if err != nil {
            return
        }
    }
    idx, _ := GetIndex(prefix + "name", job.Name)
    if idx > 0 && idx != job.Id {
        err = errors.New("Duplicate Job name: " + job.Name)
        return
    }
    key = tableName + ":" + strconv.FormatInt(job.Id, 10)
    err = SetObject(key, job)
    if err == nil {
        if e := AddIndex(tableName, strconv.FormatInt(job.Id, 10), job.Id); e != nil {
            log.Printf("AddIndex Error: %s %d\n", tableName, job.Id)
        }
        if e := AddIndex(prefix + job.Status + ":sched", strconv.FormatInt(job.Id, 10), job.SchedAt); e != nil {
            log.Printf("AddIndex Error: %s %d\n", prefix + job.Status + ":sched", job.Id)
        }
        if e := AddIndex(prefix + "name", job.Name, job.Id); e != nil {
            log.Printf("DelIndex Error: %s %s\n",  prefix + "name", job.Name)
        }
    }
    return
}


func (job *Job) Delete() (err error) {
    var tableName = GetTableName(*job)
    var key = tableName + ":" + strconv.FormatInt(job.Id, 10)
    var prefix = tableName + ":" + job.Func + ":"
    err = DelObject(key)
    DelIndex(tableName, strconv.FormatInt(job.Id, 10))
    DelIndex(prefix + job.Status + ":sched", strconv.FormatInt(job.Id, 10))
    DelIndex(prefix + "name", job.Name)
    return
}


func GetJob(id int64) (job Job, err error) {
    var tableName = GetTableName(job)
    var key = tableName + ":" +  strconv.FormatInt(id, 10)
    err = GetObject(key, &job)
    return
}


func DelJob(id int64) (err error) {
    var job Job
    job, err = GetJob(id)
    if err != nil {
        return err
    }
    err = job.Delete()
    return err
}


func CountJob() (count int64, err error) {
    var tableName = GetTableName(Job{})
    count, err = CountIndex(tableName)
    return
}


func RangeJob(start, stop int, rev ...bool) (jobs []Job, err error) {
    var tableName = GetTableName(Job{})
    var idxs []Index
    idxs, err = RangeIndex(tableName, start, stop, rev...)
    jobs = make([]Job, len(idxs))

    for k, idx := range idxs {
        job, _ :=  GetJob(idx.Score)
        job.Id = idx.Score
        jobs[k] = job
    }
    return
}


func RangeSchedJob(Func, status string, start, stop int) (jobs []Job, err error) {
    var tableName = GetTableName(Job{})
    var prefix = tableName + ":" + Func + ":"
    var idxs []Index
    idxs, err = RangeIndex(prefix + status + ":sched", start, stop)
    jobs = make([]Job, len(idxs))

    for k, idx := range idxs {
        jobId, _ := strconv.ParseInt(idx.Name, 10, 0)
        job, _ :=  GetJob(jobId)
        job.Id = jobId
        job.Status = status
        jobs[k] = job
    }
    return
}


func CountSchedJob(Func, status string) (count int64, err error) {
    var tableName = GetTableName(Job{})
    var prefix = tableName + ":" + Func + ":"
    count, err = CountIndex(prefix + status + ":sched")
    return
}
