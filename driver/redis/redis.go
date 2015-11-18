package redis

import (
	"errors"
	"fmt"
	"github.com/Lupino/periodic/driver"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/groupcache/lru"
	"log"
	"strconv"
	"strings"
	"sync"
)

// PREFIX the redis key prefix
const PREFIX = "periodic:job:"

// Driver define a redis store driver
type Driver struct {
	pool     *redis.Pool
	RWLocker *sync.Mutex
	cache    *lru.Cache
}

// NewDriver create a redis driver
func NewDriver(server string) Driver {
	parts := strings.SplitN(server, "://", 2)
	pool := redis.NewPool(func() (conn redis.Conn, err error) {
		conn, err = redis.Dial("tcp", parts[1])
		return
	}, 3)
	var cache *lru.Cache
	cache = lru.New(1000)
	var RWLocker = new(sync.Mutex)

	return Driver{pool: pool, cache: cache, RWLocker: RWLocker}
}

func (r Driver) get(jobID int64) (job driver.Job, err error) {
	var data []byte
	var conn = r.pool.Get()
	defer conn.Close()
	var key = PREFIX + strconv.FormatInt(jobID, 10)
	if val, hit := r.cache.Get(key); hit {
		return val.(driver.Job), nil
	}
	data, err = redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return
	}
	job, err = driver.NewJob(data)
	if err == nil {
		r.cache.Add(key, job)
	}
	return
}

// Save job. when job is exists update it, other create one.
func (r Driver) Save(job *driver.Job, force ...bool) (err error) {
	defer r.RWLocker.Unlock()
	r.RWLocker.Lock()
	var key string
	var prefix = PREFIX + job.Func + ":"
	var conn = r.pool.Get()
	defer conn.Close()
	if job.ID > 0 && (len(force) == 0 || !force[0]) {
		old, e := r.get(job.ID)
		key = PREFIX + strconv.FormatInt(job.ID, 10)
		if e != nil || old.ID < 1 {
			err = fmt.Errorf("Update Job %d fail, the old job is not exists.", job.ID)
			return
		}
		r.cache.Remove(key)
		if old.Name != job.Name {
			if _, e := conn.Do("ZERM", prefix+"name", old.Name); e != nil {
				log.Printf("Error: ZREM %s %s failed\n", prefix+"name", old.Name)
			}
		}
	} else {
		job.ID, err = redis.Int64(conn.Do("INCRBY", PREFIX+"sequence", 1))
		if err != nil {
			return
		}
	}
	idx, _ := redis.Int64(conn.Do("ZSCORE", prefix+"name", job.Name))
	if idx > 0 && idx != job.ID {
		err = errors.New("Duplicate Job name: " + job.Name)
		return
	}
	key = PREFIX + strconv.FormatInt(job.ID, 10)
	_, err = conn.Do("SET", key, job.Bytes())
	if err == nil {
		if _, e := conn.Do("ZADD", prefix+"name", job.ID, job.Name); e != nil {
			log.Printf("Error: ZADD %s %d %s fail\n", prefix+"name", job.ID, job.Name)
		}
		if _, e := conn.Do("ZADD", PREFIX+"ID", job.ID, strconv.FormatInt(job.ID, 10)); e != nil {
			log.Printf("Error: ZADD %s %d %d fail\n", PREFIX+"ID", job.ID, job.ID)
		}
	}
	return
}

// Delete a job with job id.
func (r Driver) Delete(jobID int64) (err error) {
	defer r.RWLocker.Unlock()
	r.RWLocker.Lock()
	var key = PREFIX + strconv.FormatInt(jobID, 10)
	job, e := r.get(jobID)
	if e != nil {
		return e
	}
	var prefix = PREFIX + job.Func + ":"

	var conn = r.pool.Get()
	defer conn.Close()

	_, err = conn.Do("DEL", key)
	conn.Do("ZREM", prefix+"name", job.Name)
	conn.Do("ZREM", PREFIX+"ID", strconv.FormatInt(job.ID, 10))
	r.cache.Remove(key)
	return
}

// Get a job with job id.
func (r Driver) Get(jobID int64) (job driver.Job, err error) {
	defer r.RWLocker.Unlock()
	r.RWLocker.Lock()
	job, err = r.get(jobID)
	return
}

// GetOne get a job with func and name.
func (r Driver) GetOne(Func string, jobName string) (job driver.Job, err error) {
	defer r.RWLocker.Unlock()
	r.RWLocker.Lock()
	var conn = r.pool.Get()
	defer conn.Close()
	jobID, _ := redis.Int64(conn.Do("ZSCORE", PREFIX+Func+":name", jobName))
	if jobID > 0 {
		return r.get(jobID)
	}
	return
}

// NewIterator create a job Iterator with func or nil.
func (r Driver) NewIterator(Func []byte) driver.Iterator {
	r.RWLocker.Lock()
	return &Iterator{
		Func:     Func,
		cursor:   0,
		cacheJob: make([]driver.Job, 0),
		start:    0,
		limit:    20,
		err:      nil,
		r:        r,
	}
}

// Close the redis driver
func (r Driver) Close() error {
	return nil
}

// Iterator define the job iterator
type Iterator struct {
	Func     []byte
	cursor   int
	err      error
	cacheJob []driver.Job
	start    int
	limit    int
	r        Driver
}

// Next advances the iterator to the next value, which will then be available through
// then the Value method. It returns false if no further advancement is possible.
func (iter *Iterator) Next() bool {
	iter.cursor++
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
		key = PREFIX + "ID"
	} else {
		key = PREFIX + string(iter.Func) + ":name"
	}

	reply, err := redis.Values(conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
	if err != nil || len(reply) == 0 {
		return false
	}
	var jobID int64
	jobs := make([]driver.Job, len(reply)/2)
	for k, v := range reply {
		if k%2 == 1 {
			jobID, _ = strconv.ParseInt(string(v.([]byte)), 10, 0)
			jobs[(k-1)/2], _ = iter.r.get(jobID)
		}
	}
	iter.cacheJob = jobs
	iter.cursor = 0
	return true
}

// Value returns the current job.
func (iter *Iterator) Value() driver.Job {
	return iter.cacheJob[iter.cursor]
}

// Error returns the current error.
func (iter *Iterator) Error() error {
	return iter.err
}

// Close the iterator
func (iter *Iterator) Close() {
	iter.r.RWLocker.Unlock()
}
