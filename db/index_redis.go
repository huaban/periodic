// +build !ledis

package db

import (
    "github.com/garyburd/redigo/redis"
    "strconv"
)


func AddIndex(name, member string, score int64) (err error) {
    var key = "index:" + name
    var conn = pool.Get()
    defer conn.Close()
    _, err = conn.Do("ZADD", PREFIX + key, score, member)
    return
}

func GetIndex(name, member string) (score int64, err error) {
    var key = "index:" + name
    var conn = pool.Get()
    defer conn.Close()
    score, err = redis.Int64(conn.Do("ZSCORE", PREFIX + key, member))
    return
}

func RangeIndex(name string, start, stop int, rev ...bool) (retval []Index, err error) {
    var key = "index:" + name
    var conn = pool.Get()
    defer conn.Close()
    cmd := "ZRANGE"
    if len(rev) > 0 {
        if rev[0] {
            cmd = "ZREVRANGE"
        }
    }
    reply, err := redis.Values(conn.Do(cmd, PREFIX + key, start, stop, "WITHSCORES"))
    var _key string
    var score int64
    retval = make([]Index, len(reply)/2)
    for k, v := range reply {
        if k % 2 == 1 {
            score, _ = strconv.ParseInt(string(v.([]byte)), 10, 0)
            retval[(k-1)/2] = Index{_key,score}
        } else {
            _key = string(v.([]byte))
        }
    }
    return
}

func CountIndex(name string) (count int64, err error) {
    var key = "index:" + name
    var conn = pool.Get()
    defer conn.Close()
    count, err = redis.Int64(conn.Do("ZCARD", PREFIX + key))
    return
}

func DropIndex(name string) (err error) {
    var key = "index:" + name
    err = DelObject(key)
    return
}

func DelIndex(name, member string) (err error) {
    var key = "index:" + name
    var conn = pool.Get()
    defer conn.Close()
    _, err = conn.Do("ZREM", PREFIX + key, member)
    return
}
