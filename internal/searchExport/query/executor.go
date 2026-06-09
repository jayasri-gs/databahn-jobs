package query

import (
	"context"
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

type QueryExecutor interface {
	Connect(ctx context.Context) error
	ExecuteUnload(ctx context.Context, query, database, s3OutputPath string) (*UnloadResult, error)
	GetAWSConfig() interface{}
	GetOutputLocation() string
	Close() error
}
