package db


import (
    "errors"
    "fmt"
    "strconv"
)


type Job struct {
    Id        int    `json:"job_id"`
    Name      string `json:"name"`
    Timeout   int    `json:"timeout"`
    SchedAt   int    `json:"sched_at"`
    Status    string `json:"status"`
}


func (job *Job) Save() (err error) {
    var tableName = GetTableName(*job)
    var key string
    if job.Id > 0 {
        var old Job
        key = tableName + ":" + strconv.Itoa(job.Id)
        err = GetObject(key, &old)
        if err != nil || old.Id < 1 {
            err = errors.New(fmt.Sprintf("Update Job %d fail, the old job is not exists.", job.Id))
            return
        }
        if old.Name != job.Name {
            DelIndex(tableName + ":name", old.Name)
        }
        if old.Status != job.Status {
            DelIndex(tableName + ":" + job.Status + ":sched", strconv.Itoa(job.Id))
        }
    } else {
        job.Id, err = NextSequence(tableName)
        if err != nil {
            return
        }
    }
    key = tableName + ":" + strconv.Itoa(job.Id)
    err = SetObject(key, job)
    if err == nil {
        AddIndex(tableName, strconv.Itoa(job.Id), job.Id)
        AddIndex(tableName + ":" + job.Status + ":sched", strconv.Itoa(job.Id), job.SchedAt)
        AddIndex(tableName + ":name", job.Name, job.Id)
    }
    return
}


func (job *Job) Delete() (err error) {
    var tableName = GetTableName(*job)
    var key = tableName + ":" + strconv.Itoa(job.Id)
    err = DelObject(key)
    DelIndex(tableName, strconv.Itoa(job.Id))
    DelIndex(tableName + ":" + job.Status + ":sched", strconv.Itoa(job.Id))
    DelIndex(tableName + ":name", job.Name)
    return
}


func GetJob(id int) (job Job, err error) {
    var tableName = GetTableName(job)
    var key = tableName + ":" +  strconv.Itoa(id)
    err = GetObject(key, &job)
    return
}


func DelJob(id int) (err error) {
    var job Job
    job, err = GetJob(id)
    if err != nil {
        return err
    }
    err = job.Delete()
    return err
}


func CountJob() (count int, err error) {
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
        jobs[k] = job
    }
    return
}


func RangeSchedJob(status string, start, stop int) (jobs []Job, err error) {
    var tableName = GetTableName(Job{})
    var idxs []Index
    idxs, err = RangeIndex(tableName + ":" + status + ":sched", start, stop)
    jobs = make([]Job, len(idxs))

    for k, idx := range idxs {
        jobId, _ := strconv.Atoi(idx.Name)
        job, _ :=  GetJob(jobId)
        jobs[k] = job
    }
    return
}


func CountSchedJob(status string) (count int, err error) {
    var tableName = GetTableName(Job{})
    count, err = CountIndex(tableName + ":" + status + ":sched")
    return
}
