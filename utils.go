package main

import (
    "os"
    "net"
    "log"
    "bytes"
    "strconv"
    "encoding/json"
    "container/list"
    "huabot-sched/db"
)


const NULL_CHAR = byte(1)


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
    buf := bytes.NewBuffer(jobStr)
    buf.WriteByte(NULL_CHAR)
    buf.WriteString(strconv.FormatInt(job.Id, 10))
    return buf.Bytes(), nil
}


func removeListJob(l *list.List, jobId int64) {
    for e := l.Front(); e != nil; e = e.Next() {
        if e.Value.(db.Job).Id == jobId {
            l.Remove(e)
            break
        }
    }
}
