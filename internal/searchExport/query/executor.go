package query

import (
	"context"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
)

const (
	QueryStateRunning   = "RUNNING"
	QueryStateQueued    = "QUEUED"
	QueryStateSucceeded = "SUCCEEDED"
	QueryStateFailed    = "FAILED"
	QueryStateCancelled = "CANCELLED"

	EngineAthena  = "ATHENA"
	EngineSynapse = "SYNAPSE"
)

type UnloadResult struct {
	OutputLocation   string
	ManifestLocation string
	BytesScanned     int64
}

// UnloadOptions selects the server-side export output format for Athena UNLOAD.
type UnloadOptions struct {
	Format    string // parquet, textfile, json
	Delimiter string // single-character delimiter for textfile (CSV)
}

type BaseExecutor interface {
	Engine() string
	Connect(ctx context.Context) error
	Close() error
	GetQueryColumns(ctx context.Context, query, database string) ([]string, error)
	GetAWSConfig() interface{}
}

type UnloadExecutor interface {
	BaseExecutor
	ExecuteUnloadAsync(ctx context.Context, query, database, outputPath string, opts UnloadOptions) (executionID string, err error)
	CheckQueryStatus(ctx context.Context, executionID string) (string, error)
	WaitForExecution(ctx context.Context, executionID string) error
	GetExecutionResult(ctx context.Context, executionID string) (*UnloadResult, error)
	CancelQueryExecution(ctx context.Context, executionID string) error
	GetOutputLocation() string
	NewStagingReader(tempDir string) (unload.StagingReader, error)
}

type RowStreamExecutor interface {
	BaseExecutor
	ValidateExportQuery(ctx context.Context, query string) error
	StreamRows(ctx context.Context, query string, opts StreamRowsOptions, fn func(row []interface{}) error) (int64, error)
	CancelQueryExecution(ctx context.Context, spid string) error
}

// QueryExecutor is an alias kept for Athena UNLOAD wiring.
type QueryExecutor = UnloadExecutor
