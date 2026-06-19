package upload

import (
	"context"
	"io"
	"time"
)

type PartInfo struct {
	PartNumber int
	ETag       string
	Size       int64
}

type CloudUploader interface {
	Init(ctx context.Context, bucket, key, contentType string) error
	UploadPart(ctx context.Context, partNumber int, data io.Reader, size int64) (*PartInfo, error)
	Complete(ctx context.Context, parts []PartInfo) error
	Abort(ctx context.Context) error
	GeneratePresignedURL(ctx context.Context, expiry time.Duration) (string, error)
	GetLocation() string
	UploadID() string
	// ListParts returns all parts already uploaded for an in-progress multipart upload.
	// Returns nil slice (not error) if the upload ID is empty.
	ListParts(ctx context.Context) ([]PartInfo, error)
}
