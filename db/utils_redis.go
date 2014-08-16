// +build !ledis

package db

import (
    "github.com/garyburd/redigo/redis"
    "encoding/json"
)

const PREFIX = "huabot-sched:"

func GetObject(key string, obj interface{}) (err error) {
    var data []byte
    var conn = pool.Get()
    defer conn.Close()
    data, err = redis.Bytes(conn.Do("GET", PREFIX + key))
    if err != nil {
        return
    }
    err = json.Unmarshal(data, obj)
    return
}

func SetObject(key string, obj interface{}) (err error) {
    var data []byte
    data, err = json.Marshal(obj)
    if err != nil {
        return
    }
    var conn = pool.Get()
    defer conn.Close()
    _, err = conn.Do("SET", PREFIX + key, data)
    return
}

func DelObject(key string) (err error) {
    var conn = pool.Get()
    defer conn.Close()
    _, err = conn.Do("DEL", PREFIX + key)
    return
}

func NextSequence(name string) (val int, err error) {
    var conn = pool.Get()
    defer conn.Close()
    val, err = redis.Int(conn.Do("INCRBY", PREFIX + "sequence:" + name, 1))
    return
}
