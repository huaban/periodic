// +build ledis

package db

import (
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
    _, err = db.Del([]byte(key))
    return
}

func NextSequence(name string) (val int64, err error) {
    val, err = db.Incr([]byte("sequence:" + name))
    return
}
