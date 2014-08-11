package db

import (
    "github.com/garyburd/redigo/redis"
    "encoding/json"
    "reflect"
    "strings"
)

func GetObject(key string, obj interface{}) (err error) {
    var data []byte
    var conn = pool.Get()
    defer conn.Close()
    data, err = redis.Bytes(conn.Do("GET", key))
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
    _, err = conn.Do("SET", key, data)
    return
}

func DelObject(key string) (err error) {
    var conn = pool.Get()
    defer conn.Close()
    _, err = conn.Do("DEL", key)
    return
}

func NextSequence(name string) (val int, err error) {
    var conn = pool.Get()
    defer conn.Close()
    val, err = redis.Int(conn.Do("INCRBY", "sequence:" + name, 1))
    return
}

func GetTableName(obj interface{}) string {
    typeof := reflect.TypeOf(obj)
    tableName := strings.ToLower(typeof.Name())
    return tableName
}
