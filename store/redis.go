package store


import (
    "fmt"
    "log"
    "errors"
    "strings"
    "strconv"
    "encoding/json"
    "huabot-sched/sched"
    "github.com/garyburd/redigo/redis"
)


const REDIS_PREFIX = "huabot-sched:job:"


type RedisStore struct {
    pool *redis.Pool
}


func NewRedisStore(server string) RedisStore {
    parts := strings.SplitN(server, "://", 2)
    pool := redis.NewPool(func() (conn redis.Conn, err error) {
        conn, err = redis.Dial("tcp", parts[1])
        return
    }, 3)

    return RedisStore{pool: pool,}
}


func (r RedisStore) get(jobId int64) (job sched.Job, err error) {
    var data []byte
    var conn = r.pool.Get()
    defer conn.Close()
    var key = REDIS_PREFIX + strconv.FormatInt(jobId, 10)
    data, err = redis.Bytes(conn.Do("GET", key))
    if err != nil {
        return
    }
    err = json.Unmarshal(data, &job)
    return
}


func (r RedisStore) Save(job *sched.Job) (err error) {
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
                log.Printf("DelIndex Error: %s %s\n", prefix + "name", old.Name)
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
    var data []byte
    data, err = json.Marshal(job)
    if err != nil {
        return
    }
    _, err = conn.Do("SET", key, data)
    if err == nil {
        if _, e := conn.Do("ZADD", prefix + "name", job.Name, job.Id); e != nil {
            log.Printf("DelIndex Error: %s %s\n",  prefix + "name", job.Name)
        }
        if _, e := conn.Do("ZADD", REDIS_PREFIX + "ID", strconv.FormatInt(job.Id, 10), job.Id); e != nil {
            log.Printf("DelIndex Error: %s %s\n",  prefix + "name", job.Name)
        }
    }
    return
}


func (r RedisStore) Delete(jobId int64) (err error) {
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


func (r RedisStore) Get(jobId int64) (job sched.Job, err error) {
    job, err = r.get(jobId)
    return
}


func (r RedisStore) GetOne(Func string, jobName string) (job sched.Job, err error) {
    var conn = r.pool.Get()
    defer conn.Close()
    jobId, _ := redis.Int64(conn.Do("ZSCORE", REDIS_PREFIX + Func + ":name", jobName))
    if jobId > 0 {
        return r.get(jobId)
    }
    return
}


func (r RedisStore) NewIterator(Func []byte) sched.JobIterator {
    return &RedisIterator{
        Func: Func,
        cursor: 0,
        cacheJob: make([]sched.Job, 0),
        start: 0,
        limit: 20,
        err: nil,
        r: r,
    }
}


type RedisIterator struct {
    Func   []byte
    cursor int
    err    error
    cacheJob []sched.Job
    start  int
    limit  int
    r      RedisStore
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
    jobs := make([]sched.Job, len(reply)/2)
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


func (iter *RedisIterator) Value() sched.Job {
    return iter.cacheJob[iter.cursor]
}


func (iter *RedisIterator) Error() error {
    return iter.err
}


func (iter *RedisIterator) Close() {

}
