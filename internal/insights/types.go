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

// NewJobResult creates a new JobResult with the given errors and success status
func NewJobResult(errors []JobError, success bool) JobResult {
	return common.NewJobResult(errors, success)
}

// NewJobResultFromError creates a JobResult from a single error
func NewJobResultFromError(err error) JobResult {
	return common.NewJobResultFromError(err)
}
