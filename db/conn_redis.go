package db

import (
    "strings"
    "github.com/garyburd/redigo/redis"
)

var pool *redis.Pool

func Connect(server string) {
    parts := strings.SplitN(server, "://", 2)
    pool = redis.NewPool(func() (conn redis.Conn, err error) {
        conn, err = redis.Dial("tcp", parts[1])
        return
    }, 3)
}
