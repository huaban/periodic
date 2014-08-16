// +build ledis

package db

import (
    "github.com/siddontang/ledisdb/ledis"
    "encoding/json"
)


func GetObject(key string, obj interface{}) (err error) {
    var data []byte
    data, err = db.Get([]byte(key))
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
    err = db.Set([]byte(key), data)
    return
}

func DelObject(key string) (err error) {
    err = db.Del([]byte(key))
    return
}

func NextSequence(name string) (val int, err error) {
    v, err := db.Incr([]byte("sequence:" + name))
    return int(v), err
}
