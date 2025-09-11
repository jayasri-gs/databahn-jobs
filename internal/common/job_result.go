package common

// JobResult represents the result of a job execution with collected errors
type JobResult struct {
	Errors []JobError
}

// JobError represents a single error that occurred during job execution
type JobError struct {
	Message string
}

// NewJobResultSuccess creates a successful JobResult with no errors
func NewJobResultSuccess() JobResult {
	return JobResult{
		Errors: []JobError{},
	}
}

// NewJobResultFromErrors creates a JobResult with the given errors (failure case)
func NewJobResultFromErrors(errors []JobError) JobResult {
	return JobResult{
		Errors: errors,
	}
}

// NewJobResultFromError creates a JobResult from a single error (failure case)
func NewJobResultFromError(err error) JobResult {
	if err == nil {
		return NewJobResultSuccess()
	}
	return JobResult{
		Errors: []JobError{{Message: err.Error()}},
	}
}
