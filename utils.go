package periodic

import (
    "os"
    "net"
    "log"
    "container/list"
    "github.com/Lupino/periodic/driver"
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


func removeListJob(l *list.List, jobId int64) {
    for e := l.Front(); e != nil; e = e.Next() {
        if e.Value.(driver.Job).Id == jobId {
            l.Remove(e)
            break
        }
    }
}
