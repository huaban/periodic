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


func packJob(job db.Job) ([]byte, error) {
    jobStr, err := json.Marshal(job)
    if err != nil {
        return nil, err
    }
    pack := data.Empty()
    pack.Set("workload", string(jobStr))
    pack.Set("job_handle", strconv.Itoa(job.Id))
    return pack.Bytes(), nil
}
