// +build ledis

package db

import (
    "github.com/siddontang/ledisdb/ledis"
    "github.com/siddontang/ledisdb/config"
)


var db *ledis.DB

func Connect(cfg *config.Config) (err error){
    l, err := ledis.Open(cfg)
    if err != nil {
        return
    }
    db, err = l.Select(0)
    return
}
