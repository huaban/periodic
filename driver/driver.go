package driver


type StoreDriver interface {
    Save(*Job) error
    Delete(jobId int64) error
    Get(jobId int64) (Job, error)
    GetOne(string, string) (Job, error)
    NewIterator([]byte) JobIterator
    Close() error
}


type Iterator interface {
    Next() bool
}


type JobIterator interface {
    Iterator
    Value() Job
    Error() error
    Close()
}
