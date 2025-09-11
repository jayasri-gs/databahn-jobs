package common

// JobResult represents the result of a job execution with collected errors
type JobResult struct {
	Errors  []JobError
	Success bool
}

// JobError represents a single error that occurred during job execution
type JobError struct {
	Message string
}

// NewJobResult creates a new JobResult with the given errors and success status
func NewJobResult(errors []JobError, success bool) JobResult {
	return JobResult{
		Errors:  errors,
		Success: success,
	}
}

// NewJobResultFromError creates a JobResult from a single error
func NewJobResultFromError(err error) JobResult {
	if err == nil {
		return JobResult{
			Errors:  []JobError{},
			Success: true,
		}
	}
	return JobResult{
		Errors:  []JobError{{Message: err.Error()}},
		Success: false,
	}
}
