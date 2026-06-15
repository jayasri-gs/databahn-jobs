package query

import (
	"context"
)

const (
	QueryStateRunning   = "RUNNING"
	QueryStateQueued    = "QUEUED"
	QueryStateSucceeded = "SUCCEEDED"
	QueryStateFailed    = "FAILED"
	QueryStateCancelled = "CANCELLED"
)

type ColumnType struct {
	Name         string
	DatabaseType string
	Nullable     bool
}

type UnloadResult struct {
	OutputLocation   string
	ManifestLocation string
	BytesScanned     int64
}

// UnloadOptions selects the Athena UNLOAD output format.
// Parquet is used for Excel; TEXTFILE/JSON skip the local conversion step for CSV/JSON exports.
type UnloadOptions struct {
	Format    string // parquet, textfile, json
	Delimiter string // single-character delimiter for textfile (CSV)
}

type QueryExecutor interface {
	Connect(ctx context.Context) error
	ExecuteUnload(ctx context.Context, query, database, s3OutputPath string, opts UnloadOptions) (*UnloadResult, error)
	// ExecuteUnloadAsync starts an UNLOAD query and returns the execution ID without waiting.
	ExecuteUnloadAsync(ctx context.Context, query, database, s3OutputPath string, opts UnloadOptions) (executionID string, err error)
	GetQueryColumns(ctx context.Context, query, database string) ([]string, error)
	GetAWSConfig() interface{}
	GetOutputLocation() string
	Close() error

	// Resume support
	CheckQueryStatus(ctx context.Context, executionID string) (string, error)
	WaitForExecution(ctx context.Context, executionID string) error
	GetExecutionResult(ctx context.Context, executionID string) (*UnloadResult, error)
	CancelQueryExecution(ctx context.Context, executionID string) error
}
