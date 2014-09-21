package periodic

import (
    "os"
    "net"
    "log"
    "fmt"
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


func removeListJob(l *list.List, jobId int64) {
    for e := l.Front(); e != nil; e = e.Next() {
        if e.Value.(Job).Id == jobId {
            l.Remove(e)
            break
        }
    }
}


func ParseCommand(payload []byte) (msgId int64, cmd Command, data []byte) {
    parts := bytes.SplitN(payload, NULL_CHAR, 3)
    if len(parts) == 1 {
        err := fmt.Sprint("ParseCommand InvalId %v\n", payload)
        panic(err)
    }
    msgId, _ = strconv.ParseInt(string(parts[0]), 10, 0)
    cmd = Command(parts[1][0])
    if len(parts) == 3 {
        data = parts[2]
    }
    return
}
