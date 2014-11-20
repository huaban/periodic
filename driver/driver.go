package driver

// Define the general store interface.

type StoreDriver interface {
    // Save job. when job is exists update it, other create one.
    Save(*Job) error
    // Delete a job with job id.
    Delete(jobId int64) error
    // Get a job with job id.
    Get(jobId int64) (Job, error)
    // Get a job with func and name.
    GetOne(string, string) (Job, error)
    // Create a JobIterator with func or nil.
    NewIterator([]byte) JobIterator
    // Close the iterator
    Close() error
}

// Define the general iterator interface.

type Iterator interface {
    // Next advances the iterator to the next value, which will then be available through
    // then the Value method. It returns false if no further advancement is possible.
    Next() bool
}


type JobIterator interface {
    Iterator
    // Returns the current job.
    Value() Job
    // Returns the current error.
    Error() error
    // Close the iterator
    Close()
}
