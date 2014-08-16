// +build ledis

package db

import (
    "github.com/siddontang/ledisdb/ledis"
)


func AddIndex(name, member string, score int) (err error) {
    var key = "index:" + name
    _, err = db.ZAdd([]byte(key), ledis.ScorePair{int64(score), []byte(member)})
    return
}

func GetIndex(name, member string) (score int, err error) {
    var key = "index:" + name
    score1, err := db.ZScore([]byte(key), []byte(name))
    return int(score1), err
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
        retval[k] = Index{Name: string(scorepair.Member), Score: int(scorepair.Score),}
    }
    return
}

func CountIndex(name string) (count int, err error) {
    var key = "index:" + name
    count1, err := db.ZCard([]byte(key))
    return int(count1), err
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
