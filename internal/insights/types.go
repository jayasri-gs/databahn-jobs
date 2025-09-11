package insights

import "github.com/databahn-ai/databahn-jobs/internal/common"

// JobResult represents the result of a job execution with collected errors
type JobResult = common.JobResult

// JobError represents a single error that occurred during job execution
type JobError = common.JobError

// ErrorCollector interface for collecting multiple errors during job execution
type ErrorCollector = common.ErrorCollector

// NewErrorCollector creates a new error collector instance
func NewErrorCollector() ErrorCollector {
	return common.NewErrorCollector()
}

// NewJobResultSuccess creates a successful JobResult with no errors
func NewJobResultSuccess() JobResult {
	return common.NewJobResultSuccess()
}

// NewJobResultFromErrors creates a JobResult with the given errors (failure case)
func NewJobResultFromErrors(errors []JobError) JobResult {
	return common.NewJobResultFromErrors(errors)
}

// NewJobResultFromError creates a JobResult from a single error (failure case)
func NewJobResultFromError(err error) JobResult {
	return common.NewJobResultFromError(err)
}
