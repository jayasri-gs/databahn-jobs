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

// UnloadOptions selects the server-side export output format (Athena UNLOAD or Synapse CETAS).
type UnloadOptions struct {
	Format    string // parquet, textfile, json
	Delimiter string // single-character delimiter for textfile (CSV)
}

type QueryExecutor interface {
	Engine() string
	Connect(ctx context.Context) error
	ExecuteUnloadAsync(ctx context.Context, query, database, outputPath string, opts UnloadOptions) (executionID string, err error)
	GetQueryColumns(ctx context.Context, query, database string) ([]string, error)
	GetAWSConfig() interface{}
	GetOutputLocation() string
	NewStagingReader(tempDir string) (unload.StagingReader, error)
	Close() error

	CheckQueryStatus(ctx context.Context, executionID string) (string, error)
	WaitForExecution(ctx context.Context, executionID string) error
	GetExecutionResult(ctx context.Context, executionID string) (*UnloadResult, error)
	CancelQueryExecution(ctx context.Context, executionID string) error
}
