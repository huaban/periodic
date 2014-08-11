package main

import (
    "os"
    "net"
    "log"
    "strconv"
    "encoding/json"
    "huabot-sched/db"
    "github.com/docker/libchan/data"
)


func sockCheck(sockFile string) {
    _, err := os.Stat(sockFile)
    if err == nil || os.IsExist(err) {
        conn, err := net.Dial("unix", sockFile)
        if err == nil {
            conn.Close()
            log.Fatal("Huabot-sched is already started.")
        }
        os.Remove(sockFile)
    }
}


func packJob(job db.Job) (pack []byte, err error) {
    jobStr, err := json.Marshal(job)
    if err != nil {
        return nil, err
    }
    pack = data.Empty().Set("workload", string(jobStr)).Set("job_handle", strconv.Itoa(job.Id)).Bytes()
    return pack, nil
}
