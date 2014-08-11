package main

import (
    "os"
    "net"
    "log"
    "time"
    "strconv"
    "math/rand"
    "github.com/garyburd/redigo/redis"
)

var pool *redis.Pool

func Connect(server string) {
    pool = redis.NewPool(func() (conn redis.Conn, err error) {
        conn, err = redis.Dial("tcp", server)
        return
    }, 3)
}


type Job struct {
    JobId string
    SchedAt int
}


func NextSchedJob(start, stop int) (retval []Job, err error) {
    var key = "huabot:robot:sched"
    var conn = pool.Get()
    defer conn.Close()
    cmd := "ZRANGE"
    reply, err := redis.Values(conn.Do(cmd, key, start, stop, "WITHSCORES"))
    if err != nil {
        return
    }
    var _key string
    var score int
    retval = make([]Job, len(reply)/2)
    for k, v := range reply {
        if k % 2 == 1 {
            score, _ = strconv.Atoi(string(v.([]byte)))
            retval[(k-1)/2] = Job{_key,score}
        } else {
            _key = string(v.([]byte))
        }
    }
    return
}


func SchedLater(job string, delay int) (err error) {
    var key = "huabot:robot:sched"
    var conn = pool.Get()
    defer conn.Close()
    now := time.Now()
    sched_at := int(now.Unix()) + delay
    _, err = conn.Do("ZADD", key, sched_at, job)
    return
}


func RandomDelay() (ret int) {
    ret = 5 + rand.Intn(200)
    return
}


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
