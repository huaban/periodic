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


func StartHttpServer(addr string) {
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

    api(mart)

    mart.RunOnAddr(addr)
}


type JobForm struct {
    Name    string `form:"name" binding:"required"`
    Timeout int    `form:"timeout"`
    SchedAt int    `form:"sched_at" binding:"required"`
}


func api(mart *martini.ClassicMartini) {

    mart.Post(API + "/jobs/", binding.Bind(JobForm{}), func(j JobForm, r render.Render) {
        job := new(db.Job)
        job.Name = j.Name
        job.Timeout = j.Timeout
        job.SchedAt = j.SchedAt
        err := job.Save()
        if err != nil {
            r.JSON(http.StatusInternalServerError, map[string]interface{}{"err": err.Error()})
            return
        }
        r.JSON(http.StatusOK, map[string]db.Job{"job": *job})
    })


    mart.Get(API + "/jobs/(?P<job_id>[0-9]+)", func(params martini.Params, r render.Render) {
        jobId, _ := strconv.Atoi(params["job_id"])
        job, err := db.GetJob(jobId)
        if err != nil {
            r.JSON(http.StatusInternalServerError, map[string]interface{}{"err": err.Error()})
            return
        }
        r.JSON(http.StatusOK, map[string]db.Job{"job": *job})
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
        var jobs []*db.Job
        var count int
        jobs, err = db.RangeSchedJob(status, start, stop)
        if err != nil {
            log.Printf("Error: RangeSchedJob error %s\n", err)
            r.JSON(http.StatusOK, map[string]interface{}{"jobs": "[]", "total": count, "current": start})
            return
        }
        r.JSON(http.StatusOK, map[string]interface{}{"jobs": jobs, "total": count, "current": start})
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
        var jobs []*db.Job
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
        var jobs []*db.Job
        var count int
        jobs, err = db.RangeJob(start, stop)
        count, _ = db.CountJob()
        if err != nil {
            log.Printf("Error: RangeSchedJob error %s\n", err)
            r.JSON(http.StatusOK, map[string]interface{}{"jobs": "[]", "total": count, "current": start})
            return
        }
        r.JSON(http.StatusOK, map[string]interface{}{"jobs": jobs, "total": count, "current": start})
    })
}
