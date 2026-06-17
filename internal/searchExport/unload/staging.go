package unload

import (
	"context"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"go.uber.org/zap"
)

type StagingReader interface {
	ListFiles(ctx context.Context, prefix string) ([]string, error)
	ParseManifest(ctx context.Context, manifestPath string) ([]string, error)
	StreamToUploader(ctx context.Context, uploader upload.CloudUploader, files []string, header []byte, format, delimiter string, log *zap.Logger, opts StreamOptions) (int64, int64, error)
	StreamRows(ctx context.Context, files []string, fn func(row []interface{}) error) error
	DeleteFiles(ctx context.Context, files []string) error
	Columns() []string
}

var _ StagingReader = (*Reader)(nil)
