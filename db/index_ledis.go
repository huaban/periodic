// +build ledis

package db

import (
    "github.com/siddontang/ledisdb/ledis"
)


func AddIndex(name, member string, score int64) (err error) {
    var key = "index:" + name
    _, err = db.ZAdd([]byte(key), ledis.ScorePair{score, []byte(member)})
    return
}

func GetIndex(name, member string) (score int64, err error) {
    var key = "index:" + name
    score, err = db.ZScore([]byte(key), []byte(name))
    return
}

func RangeIndex(name string, start, stop int, rev ...bool) (retval []Index, err error) {
    var key = "index:" + name
    var scorepairs []ledis.ScorePair
    if len(rev) > 0 && rev[0] {
        scorepairs, err = db.ZRevRange([]byte(key), start, stop)
    } else {
        scorepairs, err = db.ZRange([]byte(key), start, stop)
    }
    if err != nil {
        return
    }
    retval = make([]Index, len(scorepairs))
    for k, scorepair := range scorepairs {
        retval[k] = Index{Name: string(scorepair.Member), Score: scorepair.Score,}
    }
    return
}

func CountIndex(name string) (count int64, err error) {
    var key = "index:" + name
    count, err = db.ZCard([]byte(key))
    return
}

func DropIndex(name string) (err error) {
    var key = "index:" + name
    err = DelObject(key)
    return
}

func DelIndex(name, member string) (err error) {
    var key = "index:" + name
    _, err = db.ZRem([]byte(key), []byte(member))
    return
}
