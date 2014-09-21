package drivers


import (
    "fmt"
    "log"
    "errors"
    "strings"
    "strconv"
    "github.com/Lupino/periodic"
    "github.com/garyburd/redigo/redis"
)


const REDIS_PREFIX = "periodic:job:"


type RedisDriver struct {
    pool *redis.Pool
}


func NewRedisDriver(server string) RedisDriver {
    parts := strings.SplitN(server, "://", 2)
    pool := redis.NewPool(func() (conn redis.Conn, err error) {
        conn, err = redis.Dial("tcp", parts[1])
        return
    }, 3)

    return RedisDriver{pool: pool,}
}


func (r RedisDriver) get(jobId int64) (job periodic.Job, err error) {
    var data []byte
    var conn = r.pool.Get()
    defer conn.Close()
    var key = REDIS_PREFIX + strconv.FormatInt(jobId, 10)
    data, err = redis.Bytes(conn.Do("GET", key))
    if err != nil {
        return
    }
    job, err = periodic.NewJob(data)
    return
}


func (r RedisDriver) Save(job *periodic.Job) (err error) {
    var key string
    var prefix = REDIS_PREFIX + job.Func + ":"
    var conn = r.pool.Get()
    defer conn.Close()
    if job.Id > 0 {
        old, e := r.get(job.Id)
        key = REDIS_PREFIX + strconv.FormatInt(job.Id, 10)
        if e != nil || old.Id < 1 {
            err = errors.New(fmt.Sprintf("Update Job %d fail, the old job is not exists.", job.Id))
            return
        }
        if old.Name != job.Name {
            if _, e := conn.Do("ZERM", prefix + "name", old.Name); e != nil {
                log.Printf("Error: ZREM %s %s failed\n", prefix + "name", old.Name)
            }
        }
    } else {
        job.Id, err = redis.Int64(conn.Do("INCRBY", REDIS_PREFIX + "sequence", 1))
        if err != nil {
            return
        }
    }
    idx, _ := redis.Int64(conn.Do("ZSCORE", prefix + "name", job.Name))
    if idx > 0 && idx != job.Id {
        err = errors.New("Duplicate Job name: " + job.Name)
        return
    }
    key = REDIS_PREFIX + strconv.FormatInt(job.Id, 10)
    _, err = conn.Do("SET", key, job.Bytes())
    if err == nil {
        if _, e := conn.Do("ZADD", prefix + "name", job.Id, job.Name); e != nil {
            log.Printf("Error: ZADD %s %d %s fail\n",  prefix + "name", job.Id, job.Name)
        }
        if _, e := conn.Do("ZADD", REDIS_PREFIX + "ID", job.Id, strconv.FormatInt(job.Id, 10)); e != nil {
            log.Printf("Error: ZADD %s %d %d fail\n",  REDIS_PREFIX + "ID", job.Id, job.Id)
        }
    }
    return
}


func (r RedisDriver) Delete(jobId int64) (err error) {
    var key = REDIS_PREFIX + strconv.FormatInt(jobId, 10)
    job, e := r.get(jobId)
    if e != nil {
        return e
    }
    var prefix = REDIS_PREFIX + job.Func + ":"

    var conn = r.pool.Get()
    defer conn.Close()

    _, err = conn.Do("DEL", key)
    conn.Do("ZREM", prefix + "name", job.Name)
    conn.Do("ZREM", REDIS_PREFIX + "ID", strconv.FormatInt(job.Id, 10))
    return
}


func (r RedisDriver) Get(jobId int64) (job periodic.Job, err error) {
    job, err = r.get(jobId)
    return
}


func (r RedisDriver) GetOne(Func string, jobName string) (job periodic.Job, err error) {
    var conn = r.pool.Get()
    defer conn.Close()
    jobId, _ := redis.Int64(conn.Do("ZSCORE", REDIS_PREFIX + Func + ":name", jobName))
    if jobId > 0 {
        return r.get(jobId)
    }
    return
}


func (r RedisDriver) NewIterator(Func []byte) periodic.JobIterator {
    return &RedisIterator{
        Func: Func,
        cursor: 0,
        cacheJob: make([]periodic.Job, 0),
        start: 0,
        limit: 20,
        err: nil,
        r: r,
    }
}


func (r RedisDriver) Close() error {
    return nil
}


type RedisIterator struct {
    Func   []byte
    cursor int
    err    error
    cacheJob []periodic.Job
    start  int
    limit  int
    r      RedisDriver
}

func (iter *RedisIterator) Next() bool {
    iter.cursor += 1
    if len(iter.cacheJob) > 0 && len(iter.cacheJob) > iter.cursor {
        return true
    }
    start := iter.start
    stop := iter.start + iter.limit - 1
    iter.start = iter.start + iter.limit
    var err error

    var conn = iter.r.pool.Get()
    defer conn.Close()
    var key string
    if iter.Func == nil {
        key = REDIS_PREFIX + "ID"
    } else {
        key = REDIS_PREFIX + string(iter.Func) + ":name"
    }

    reply, err := redis.Values(conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
    if err != nil || len(reply) == 0 {
        return false
    }
    var jobId int64
    jobs := make([]periodic.Job, len(reply)/2)
    for k, v := range reply {
        if k % 2 == 1 {
            jobId, _ = strconv.ParseInt(string(v.([]byte)), 10, 0)
            jobs[(k - 1) / 2], _ = iter.r.get(jobId)
        }
    }
    iter.cacheJob = jobs
    iter.cursor = 0
    return true
}


func (iter *RedisIterator) Value() periodic.Job {
    return iter.cacheJob[iter.cursor]
}


func (iter *RedisIterator) Error() error {
    return iter.err
}


func (iter *RedisIterator) Close() {

}
