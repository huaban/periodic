package sched

import (
    "os"
    "net"
    "log"
    "bytes"
    "strconv"
    "container/list"
)


func sockCheck(sockFile string) {
    _, err := os.Stat(sockFile)
    if err == nil || os.IsExist(err) {
        conn, err := net.Dial("unix", sockFile)
        if err == nil {
            conn.Close()
            log.Fatal("Periodic task system is already started.")
        }
        os.Remove(sockFile)
    }
}


func PackJob(job Job) ([]byte, error) {
    buf := bytes.NewBuffer(job.Bytes())
    buf.Write(NULL_CHAR)
    buf.WriteString(strconv.FormatInt(job.Id, 10))
    return buf.Bytes(), nil
}


func removeListJob(l *list.List, jobId int64) {
    for e := l.Front(); e != nil; e = e.Next() {
        if e.Value.(Job).Id == jobId {
            l.Remove(e)
            break
        }
    }
}
