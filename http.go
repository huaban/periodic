package main


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
    Timeout int    `form:"timeout"`
    SchedAt int    `form:"sched_at" binding:"required"`
}


func api(mart *martini.ClassicMartini, sched *Sched) {

    mart.Post(API + "/jobs/", binding.Bind(JobForm{}), func(j JobForm, r render.Render) {
        job := db.Job{
            Name: j.Name,
            Timeout: j.Timeout,
            SchedAt: j.SchedAt,
            Status: "ready",
        }
        jobId, _ := db.GetIndex("job:name", job.Name)
        if jobId > 0 {
            job.Id = jobId
        }
        err := job.Save()
        if err != nil {
            r.JSON(http.StatusInternalServerError, map[string]interface{}{"err": err.Error()})
            return
        }
        sched.Notify()
        r.JSON(http.StatusOK, map[string]db.Job{"job": job})
    })


    mart.Get(API + "/jobs/(?P<status>ready|doing)/", func(params martini.Params, req *http.Request, r render.Render) {
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
        var count int
        jobs, err = db.RangeSchedJob(status, start, stop)
        count, _ = db.CountSchedJob(status)
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
        var jobId int
        if qs.Get("id_type") == "name" {
            jobId, _ = db.GetIndex("job:name", id)
        } else {
            jobId, _ = strconv.Atoi(id)
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
        var count int
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
        var jobId int
        if qs.Get("id_type") == "name" {
            jobId, _ = db.GetIndex("job:name", id)
        } else {
            jobId, _ = strconv.Atoi(id)
        }
        err := db.DelJob(jobId)
        if err != nil {
            log.Printf("Error: DelJob error %s\n", err)
            r.JSON(http.StatusOK, map[string]interface{}{"err": err.Error()})
            return
        }
        sched.Notify()
        r.JSON(http.StatusOK, map[string]interface{}{})
    })


    mart.Post(API + "/notify", func(r render.Render) {
        sched.Notify()
        r.JSON(http.StatusOK, map[string]interface{}{})
    })


    mart.Post(API + "/status", func(r render.Render) {
        var status = make(map[string]int)
        var count int
        count, _ = db.CountJob()
        status["total"] = count
        count, _ = db.CountSchedJob("ready")
        status["ready"] = count
        count, _ = db.CountSchedJob("doing")
        status["doing"] = count
        r.JSON(http.StatusOK, status)
    })
}
