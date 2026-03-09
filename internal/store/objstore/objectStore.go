package objstore

import (
	"context"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/objectstore"
)

const (
	BucketEvents    = "events"
	BucketArtifacts = "artifacts"
)

var (
	client objectstore.ObjectStore
	cfg    configuration.ConfigReader
)

func Connect(ctx context.Context, appConfig configuration.ConfigReader) error {
	store, err := objectstore.NewObjectStore(ctx, appConfig)
	if err != nil {
		return err
	}
	client = store
	cfg = appConfig
	return nil
}

func GetClient() objectstore.ObjectStore {
	if client == nil {
		panic("object store client not initialized, call Connect first")
	}
	return client
}

// GetBucket returns the bucket/container name for the given bucket type.
// Resolves object.* keys with fallback to s3.* keys.
func GetBucket(bucketType string) string {
	if cfg == nil {
		panic("object store not initialized, call Connect first")
	}
	switch bucketType {
	case BucketEvents:
		if b := cfg.GetString(configuration.ObjectEventCollection); b != "" {
			return b
		}
		return cfg.GetString(configuration.BackupEventsS3Bucket)
	case BucketArtifacts:
		if b := cfg.GetString(configuration.ObjectArtifactCollection); b != "" {
			return b
		}
		return cfg.GetString(configuration.ArtifactsS3Bucket)
	}
	return cfg.GetString(configuration.ArtifactsS3Bucket)
}
