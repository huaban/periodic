package sched


import (
    "log"
    "strconv"
    "net/http"
    "huabot-sched/db"
    "github.com/go-martini/martini"
    "github.com/martini-contrib/render"
    "github.com/martini-contrib/binding"
)


const API = ""


func StartHttpServer(addr string, sched *Sched) {
    mart := martini.Classic()
    mart.Use(render.Renderer(render.Options{
        Directory: "templates",
        Layout: "layout",
        Extensions: []string{".tmpl", ".html"},
        Charset: "UTF-8",
        IndentJSON: true,
        IndentXML: true,
        HTMLContentType: "application/xhtml+xml",
    }))

    api(mart, sched)

    mart.RunOnAddr(addr)
}


type JobForm struct {
    Name    string `form:"name" binding:"required"`
    Func    string `form:"func" binding:"required"`
    Args    string `form:"workload"`
    Timeout int64  `form:"timeout"`
    SchedAt int64  `form:"sched_at" binding:"required"`
}


func api(mart *martini.ClassicMartini, sched *Sched) {

    mart.Post(API + "/jobs/", binding.Bind(JobForm{}), func(j JobForm, r render.Render) {
        job := db.Job{
            Name: j.Name,
            Func: j.Func,
            Args: j.Args,
            Timeout: j.Timeout,
            SchedAt: j.SchedAt,
            Status: db.JOB_STATUS_READY,
        }
        is_new := true
        jobId, _ := db.GetIndex("job:" + job.Func + ":name", job.Name)
        if jobId > 0 {
            job.Id = jobId
            if oldJob, err := db.GetJob(jobId); err == nil && oldJob.Status == db.JOB_STATUS_PROC {
                sched.DecrStatProc(oldJob)
            }
            is_new = false
        }
        err := job.Save()
        if err != nil {
            r.JSON(http.StatusInternalServerError, map[string]interface{}{"err": err.Error()})
            return
        }
        if is_new {
            sched.IncrStatJob(job)
        }
        sched.Notify()
        r.JSON(http.StatusOK, map[string]db.Job{"job": job})
    })


    mart.Get(API + "/jobs/(?P<func>[^/]+)/(?P<status>ready|doing)/",
            func(params martini.Params, req *http.Request, r render.Render) {
        Func := params["func"]
        status := params["status"]
        qs := req.URL.Query()
        var start, limit, stop int
        var err error
        if start, err = strconv.Atoi(qs.Get("start")); err != nil {
            start = 0
        }
        if limit, err = strconv.Atoi(qs.Get("limit")); err != nil {
            limit = 10
        }
        stop = start + limit
        var jobs []db.Job
        var count int64
        jobs, err = db.RangeSchedJob(Func, status, start, stop)
        count, _ = db.CountSchedJob(Func, status)
        if err != nil {
            log.Printf("Error: RangeSchedJob error %s\n", err)
            r.JSON(http.StatusOK, map[string]interface{}{"jobs": "[]", "total": count, "current": start})
            return
        }
        r.JSON(http.StatusOK, map[string]interface{}{"jobs": jobs, "total": count, "current": start})
    })


    mart.Get(API + "/jobs/(?P<job_id>[0-9a-zA-Z]+)",
            func(params martini.Params, req *http.Request, r render.Render) {
        qs := req.URL.Query()
        id := params["job_id"]
        var jobId int64
        if qs.Get("id_type") == "name" {
            Func := qs.Get("func")
            jobId, _ = db.GetIndex("job:" + Func + ":name", id)
        } else {
            jobId, _ = strconv.ParseInt(id, 10 ,0)
        }
        job, err := db.GetJob(jobId)
        if err != nil {
            r.JSON(http.StatusNotFound, map[string]interface{}{"err": err.Error()})
            return
        }
        r.JSON(http.StatusOK, map[string]db.Job{"job": job})
    })


    mart.Get(API + "/jobs/", func(req *http.Request, r render.Render) {
        qs := req.URL.Query()
        var start, limit, stop int
        var err error
        if start, err = strconv.Atoi(qs.Get("start")); err != nil {
            start = 0
        }
        if limit, err = strconv.Atoi(qs.Get("limit")); err != nil {
            limit = 10
        }
        stop = start + limit
        var jobs []db.Job
        var count int64
        jobs, err = db.RangeJob(start, stop)
        count, _ = db.CountJob()
        if err != nil {
            log.Printf("Error: RangeJob error %s\n", err)
            r.JSON(http.StatusOK, map[string]interface{}{"jobs": "[]", "total": count, "current": start})
            return
        }
        r.JSON(http.StatusOK, map[string]interface{}{"jobs": jobs, "total": count, "current": start})
    })


    mart.Delete(API + "/jobs/(?P<job_id>[0-9a-zA-Z]+)",
            func(params martini.Params, req *http.Request, r render.Render) {
        qs := req.URL.Query()
        id := params["job_id"]
        var jobId int64
        if qs.Get("id_type") == "name" {
            Func := qs.Get("func")
            jobId, _ = db.GetIndex("job:" + Func + ":name", id)
        } else {
            jobId, _ = strconv.ParseInt(id, 10, 0)
        }
        job, err := db.GetJob(jobId)
        if err != nil {
            log.Printf("Error: DelJob error %s\n", err)
            r.JSON(http.StatusOK, map[string]interface{}{"err": err.Error()})
            return
        }
        err = job.Delete()
        if err != nil {
            log.Printf("Error: DelJob error %s\n", err)
            r.JSON(http.StatusOK, map[string]interface{}{"err": err.Error()})
            return
        }
        if job.Status == db.JOB_STATUS_PROC {
            sched.DecrStatProc(job)
        }
        sched.DecrStatJob(job)
        sched.Notify()
        r.JSON(http.StatusOK, map[string]interface{}{})
    })


    mart.Post(API + "/notify", func(r render.Render) {
        sched.Notify()
        r.JSON(http.StatusOK, map[string]interface{}{})
    })


    mart.Get(API + "/(?P<func>[^/]+)/status", func(params martini.Params, r render.Render) {
        var status = make(map[string]int64)
        Func := params["func"]
        var count int64
        count, _ = db.CountJob()
        status["total"] = count
        count, _ = db.CountSchedJob(Func, db.JOB_STATUS_READY)
        status["ready"] = count
        count, _ = db.CountSchedJob(Func, db.JOB_STATUS_PROC)
        status["doing"] = count
        status["worker_count"] = int64(sched.TotalWorkerCount)
        r.JSON(http.StatusOK, status)
    })


    mart.Get(API + "/status", func(params martini.Params, r render.Render) {
        r.JSON(http.StatusOK, sched.Funcs)
    })
}
