package store


import (
    "os"
    "fmt"
    "log"
    "errors"
    "strconv"
    "encoding/json"
    "huabot-sched/sched"
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/util"
    "github.com/syndtr/goleveldb/leveldb/iterator"
)


const PRE_JOB = "job:"
const PRE_JOB_FUNC = "func:"
const PRE_SEQUENCE = "sequence:"


type LevelDBStore struct {
    db *leveldb.DB
}


func NewLevelDBStore(dbpath string) sched.Storer {
    var db *leveldb.DB
    var err error

    _, err = os.Stat(dbpath)

    if err == nil || os.IsExist(err) {
        db, err = leveldb.RecoverFile(dbpath, nil)
    } else {
        db, err = leveldb.OpenFile(dbpath, nil)
    }
    if err != nil {
        log.Fatal(err)
    }
    return LevelDBStore{
        db: db,
    }
}


func (l LevelDBStore) Save(job *sched.Job) (err error) {
    batch := new(leveldb.Batch)
    if job.Id > 0 {
        old, e := l.Get(job.Id)
        if e != nil || old.Id == 0 {
            err = errors.New(fmt.Sprintf("Update Job %d fail, the old job is not exists.", job.Id))
            return
        }
        if old.Name != job.Name {
            batch.Delete([]byte(PRE_JOB_FUNC + job.Func + ":" + old.Name))
            batch.Put([]byte(PRE_JOB_FUNC + job.Func + ":" + job.Name), []byte(strconv.FormatInt(job.Id, 10)))
        }
    } else {
        last_id, e := l.db.Get([]byte(PRE_SEQUENCE + "JOB"), nil)
        if e != nil || last_id == nil {
            job.Id = 1
        } else {
            id, _ := strconv.ParseInt(string(last_id), 10, 64)
            job.Id = id + 1
        }
        batch.Put([]byte(PRE_SEQUENCE + "JOB"), []byte(strconv.FormatInt(job.Id, 10)))
        batch.Put([]byte(PRE_JOB_FUNC + job.Func + ":" + job.Name), []byte(strconv.FormatInt(job.Id, 10)))
    }
    var data []byte
    data, err = json.Marshal(job)
    if err != nil {
        return
    }
    batch.Put([]byte(PRE_JOB + strconv.FormatInt(job.Id, 10)), data)
    err = l.db.Write(batch, nil)
    return
}


func (l LevelDBStore) Delete(jobId int64) (err error) {
    var job sched.Job
    batch := new(leveldb.Batch)
    job, err = l.Get(jobId)
    if err != nil {
        return
    }
    batch.Delete([]byte(PRE_JOB_FUNC + job.Func + ":" + job.Name))
    batch.Delete([]byte(PRE_JOB + strconv.FormatInt(job.Id, 10)))
    err = l.db.Write(batch, nil)
    return
}


func (l LevelDBStore) Get(jobId int64) (job sched.Job, err error) {
    var data []byte
    var key = PRE_JOB + strconv.FormatInt(jobId, 10)
    data, err = l.db.Get([]byte(key), nil)
    if err != nil {
        return
    }
    err = json.Unmarshal(data, &job)
    return
}


func (l LevelDBStore) GetOne(Func, name string) (job sched.Job, err error) {
    var data []byte
    var key = PRE_JOB_FUNC + Func + ":" + name
    data, err = l.db.Get([]byte(key), nil)
    if err != nil {
        return
    }
    key = PRE_JOB + string(data)
    data, err = l.db.Get([]byte(key), nil)
    if err != nil {
        return
    }
    err = json.Unmarshal(data, &job)
    return
}


func (l LevelDBStore) NewIterator(Func []byte) sched.JobIterator {
    var prefix []byte
    if Func == nil {
        prefix = []byte(PRE_JOB)
    } else {
        prefix = []byte(PRE_JOB_FUNC + string(Func))
    }
    iter := l.db.NewIterator(util.BytesPrefix(prefix), nil)
    return &LevelDBIterator{
        l: l,
        iter: iter,
    }
}


func (l LevelDBStore) Close() error {
    err := l.db.Close()
    return err
}


type LevelDBIterator struct {
    l LevelDBStore
    iter iterator.Iterator
}


func (iter *LevelDBIterator) Next() bool {
    return iter.iter.Next()
}


func (iter *LevelDBIterator) Value() (job sched.Job) {
    data := iter.iter.Value()
    jobId, _ := strconv.ParseInt(string(data), 10, 64)
    job, _ = iter.l.Get(jobId)
    return
}


func (iter *LevelDBIterator) Error() error {
    return iter.iter.Error()
}


func (iter *LevelDBIterator) Close() {
    iter.iter.Release()
}
