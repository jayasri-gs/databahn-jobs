package objectstore

import (
	"context"
	"io"
	"time"
)

// BackendType represents the object storage backend.
const (
	BackendS3   = "s3"
	BackendBlob = "blob"
)

// ObjectInfo contains metadata about an object in the store.
type ObjectInfo struct {
	Key          string
	LastModified time.Time
}

// ObjectStore defines the interface for object storage operations.
// It supports S3 and Azure Blob Storage as backends.
type ObjectStore interface {
	// Get retrieves an object from the store.
	// container is the bucket name (S3) or container name (Azure Blob).
	// key is the object path/key.
	Get(ctx context.Context, container, key string) ([]byte, error)

	// Put uploads or overwrites an object in the store.
	Put(ctx context.Context, container, key string, data []byte) error

	// PutStream uploads or overwrites an object from an io.Reader.
	// Use this for large files to avoid loading entire content into memory.
	PutStream(ctx context.Context, container, key string, reader io.Reader) error

	// Post creates a new object or appends to an existing one.
	// For S3: same as Put (create/overwrite).
	// For Azure Blob: appends to an append blob if supported.
	Post(ctx context.Context, container, key string, data []byte) error

	// Delete removes an object from the store.
	Delete(ctx context.Context, container, key string) error

	// List returns all objects in the container with the given prefix.
	// Returns object keys and their last modified timestamps.
	List(ctx context.Context, container, prefix string) ([]ObjectInfo, error)

	// GetPresignedURL generates a presigned URL for downloading an object.
	// The URL is valid for the specified expiry duration.
	// For S3: uses presigned GetObject URL.
	// For Azure Blob: generates a SAS URL with read permission.
	GetPresignedURL(ctx context.Context, container, key string, expiry time.Duration) (string, error)
}
