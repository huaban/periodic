package main

import (
    "os"
    "net"
    "log"
    "bytes"
    "errors"
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
    buf.WriteString(strconv.Itoa(job.Id))
    return buf.Bytes(), nil
}


func removeListJob(l *list.List, jobId int) {
    for e := l.Front(); e != nil; e = e.Next() {
        if e.Value.(db.Job).Id == jobId {
            l.Remove(e)
            break
        }
    }
}


func makeHeader(data []byte) ([]byte, error) {
    header := make([]byte, 4)

    length := uint32(len(data))

    if length > 0x7fffffff {
        return nil, errors.New("Data to large")
    }

    header[0] = byte((length >> 24) & 0xff)
    header[1] = byte((length >> 16) & 0xff)
    header[2] = byte((length >> 8) & 0xff)
    header[3] = byte((length >> 0) & 0xff)

    return header, nil
}


func parseHeader(header []byte) (uint32) {
    length := uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])
    length = length & ^uint32(0x80000000)

    return length
}
