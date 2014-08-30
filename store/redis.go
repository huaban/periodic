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


func (r RedisStorer) Next(Func string) (job sched.Job, err error) {
    jobs, err := db.RangeSchedJob(Func, db.JOB_STATUS_READY, 0, 0)
    if len(jobs) == 0 {
        return job, err
    }
    job = sched.Job(jobs[0])
    return job, err
}


func (r RedisStorer) GetOneByFunc(Func string, jobName string) (job sched.Job, err error) {
    jobId, _ := db.GetIndex("job:" + Func + ":name", jobName)
    if jobId > 0 {
        dbJob, err := db.GetJob(jobId)
        job = sched.Job(dbJob)
        return job, err
    }
    return
}

func (r RedisStorer) GetAll(start int64, stop int64) (jobs []sched.Job, err error) {
    dbJobs, err := db.RangeJob(int(start), int(stop))
    if err != nil {
        return jobs, err
    }
    jobs = make([]sched.Job, len(dbJobs))
    for idx, job := range dbJobs {
        jobs[idx] = sched.Job(job)
    }
    return
}


func (r RedisStorer) GetAllByFunc(Func string, jobStatus string, start int64, stop int64) (jobs []sched.Job, err error) {
    dbJobs, err := db.RangeSchedJob(Func, jobStatus, int(start), int(stop))
    if err != nil {
        return jobs, err
    }
    jobs = make([]sched.Job, len(dbJobs))
    for idx, job := range dbJobs {
        jobs[idx] = sched.Job(job)
    }
    return
}
