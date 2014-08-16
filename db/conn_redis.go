package db

import (
    "github.com/garyburd/redigo/redis"
)

var pool *redis.Pool

func Connect(server string) {
    pool = redis.NewPool(func() (conn redis.Conn, err error) {
        conn, err = redis.Dial("tcp", server)
        return
    }, 3)
}
