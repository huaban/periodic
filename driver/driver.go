package driver

// StoreDriver define the general store interface.
type StoreDriver interface {
	// Save job. when job is exists update it, other create one.
	Save(*Job, ...bool) error
	// Delete a job with job id.
	Delete(jobID int64) error
	// Get a job with job id.
	Get(jobID int64) (Job, error)
	// GetOne get a job with func and name.
	GetOne(string, string) (Job, error)
	// NewIterator create a job Iterator with func or nil.
	NewIterator([]byte) Iterator
	// Close the driver
	Close() error
}

// Iterator define the job iterator interface
type Iterator interface {
	// Next advances the iterator to the next value, which will then be available through
	// then the Value method. It returns false if no further advancement is possible.
	Next() bool
	// Value returns the current job.
	Value() Job
	// Error returns the current error.
	Error() error
	// Close the iterator
	Close()
}
