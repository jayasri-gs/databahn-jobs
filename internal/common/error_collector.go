package common

// ErrorCollector interface for collecting multiple errors during job execution
type ErrorCollector interface {
	AddError(message string)
	GetErrors() []JobError
	HasErrors() bool
}

// errorCollector implements the ErrorCollector interface
type errorCollector struct {
	errors []JobError
}

// NewErrorCollector creates a new error collector instance
func NewErrorCollector() ErrorCollector {
	return &errorCollector{
		errors: make([]JobError, 0),
	}
}

// AddError adds a new error to the collector
func (ec *errorCollector) AddError(message string) {
	ec.errors = append(ec.errors, JobError{Message: message})
}

// GetErrors returns all collected errors
func (ec *errorCollector) GetErrors() []JobError {
	return ec.errors
}

// HasErrors returns true if any errors have been collected
func (ec *errorCollector) HasErrors() bool {
	return len(ec.errors) > 0
}
