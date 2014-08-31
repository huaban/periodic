package store


import (
    "huabot-sched/db"
    "huabot-sched/sched"
)


type RedisStorer struct {}


func (r RedisStorer) Save(job sched.Job) error {
    dbJob := db.Job(job)
    return dbJob.Save()
}


func (r RedisStorer) Delete(jobId int64) error {
    return db.DelJob(jobId)
}


func (r RedisStorer) Get(jobId int64) (job sched.Job, err error) {
    var dbJob db.Job
    dbJob, err = db.GetJob(jobId)
    job = sched.Job(dbJob)
    return
}


func (r RedisStorer) Count() (int64, error) {
    return db.CountJob()
}


func (r RedisStorer) GetOne(Func string, jobName string) (job sched.Job, err error) {
    jobId, _ := db.GetIndex("job:" + Func + ":name", jobName)
    if jobId > 0 {
        dbJob, err := db.GetJob(jobId)
        job = sched.Job(dbJob)
        return job, err
    }
    return
}


func (r RedisStorer) NewIterator(Func, Status []byte) sched.JobIterator {
    return &RedisIterator{
        Func: Func,
        Status: Status,
        cursor: 0,
        cacheJob: make([]sched.Job, 0),
        start: 0,
        limit: 20,
        err: nil,
        curStatus: sched.JOB_STATUS_READY,
    }
}


type RedisIterator struct {
    Func   []byte
    Status []byte
    cursor int
    err    error
    cacheJob []sched.Job
    start  int
    limit  int
    curStatus string
}

func (iter *RedisIterator) Next() bool {
    iter.cursor += 1
    if len(iter.cacheJob) > 0 && len(iter.cacheJob) > iter.cursor {
        return true
    }
    start := iter.start
    stop := iter.start + iter.limit - 1
    iter.start = iter.start + iter.limit
    var err error
    var dbJobs []db.Job
    if iter.Func == nil && iter.Status == nil {
        dbJobs, err = db.RangeJob(start, stop)
    } else if iter.Status == nil {
        dbJobs, err = db.RangeSchedJob(string(iter.Func), iter.curStatus, start, stop)
        if len(dbJobs) == 0  && iter.curStatus == sched.JOB_STATUS_READY {
            start = 0
            stop = iter.limit - 1
            iter.start = iter.limit
            dbJobs, err = db.RangeSchedJob(string(iter.Func), sched.JOB_STATUS_PROC, start, stop)
        }
    } else {
        dbJobs, err = db.RangeSchedJob(string(iter.Func), string(iter.Status), start, stop)
    }
    if err != nil {
        iter.err = err
        return false
    }
    if len(dbJobs) == 0 {
        return false
    }
    jobs := make([]sched.Job, len(dbJobs))
    for idx, job := range dbJobs {
        jobs[idx] = sched.Job(job)
    }
    iter.cacheJob = jobs
    iter.cursor = 0
    return true
}


func (iter *RedisIterator) Value() sched.Job {
    return iter.cacheJob[iter.cursor]
}


func (iter *RedisIterator) Error() error {
    return iter.err
}


func (iter *RedisIterator) Close() {

}
