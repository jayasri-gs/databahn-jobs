package objectstore

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/databahn-ai/common-utils/configuration"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

// NewObjectStore creates an ObjectStore based on configuration.
// The backend is selected via object.backend: "s3", "blob", or "gcs".
func NewObjectStore(ctx context.Context, cfg configuration.ConfigReader) (ObjectStore, error) {
	backend := strings.ToLower(strings.TrimSpace(cfg.GetString(configuration.ObjectBackend)))
	if backend == "" {
		return nil, fmt.Errorf("object.backend is required (s3, blob, or gcs)")
	}

	switch backend {
	case BackendS3:
		return newS3StoreFromConfig(ctx, cfg)
	case BackendBlob:
		return newBlobStoreFromConfig(ctx, cfg)
	case BackendGcs:
		return newGcsStoreFromConfig(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported object backend: %q (use s3, blob, or gcs)", backend)
	}
}

func newS3StoreFromConfig(ctx context.Context, cfg configuration.ConfigReader) (ObjectStore, error) {
	region := cfg.GetString(configuration.ObjectS3Region)
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		region = "us-east-1"
	}
	endpoint := cfg.GetString(configuration.ObjectS3Endpoint)
	accessKey := cfg.GetString(configuration.ObjectS3AccessKey)
	secretKey := cfg.GetString(configuration.ObjectS3SecretKey)
	forcePathStyle := cfg.GetBool(configuration.ObjectS3ForcePathStyle)

	// Use native auth (IAM role, env vars, etc.) when keys not provided
	useNativeAuth := accessKey == "" || secretKey == ""
	logging.GetLoggerWithContext(ctx).Info("creating S3 object store",
		zap.String("region", region),
		zap.Bool("custom_endpoint", endpoint != ""),
		zap.Bool("native_auth", useNativeAuth),
	)

	return NewS3Backend(ctx, region, endpoint, accessKey, secretKey, forcePathStyle)
}

func newBlobStoreFromConfig(ctx context.Context, cfg configuration.ConfigReader) (ObjectStore, error) {
	connectionString := cfg.GetString(configuration.ObjectBlobConnectionString)
	accountName := cfg.GetString(configuration.ObjectBlobAccountName)
	accountKey := cfg.GetString(configuration.ObjectBlobAccountKey)

	// NewBlobBackend falls back to AZURE_STORAGE_* env vars when empty

	// Use native auth (managed identity, Azure CLI, etc.) when keys not provided
	useNativeAuth := connectionString == "" && accountKey == "" && accountName != ""
	logging.GetLoggerWithContext(ctx).Info("creating Azure Blob object store",
		zap.Bool("connection_string", connectionString != ""),
		zap.Bool("account_key", accountKey != ""),
		zap.Bool("native_auth", useNativeAuth),
	)

	return NewBlobBackend(ctx, connectionString, accountName, accountKey)
}

func newGcsStoreFromConfig(ctx context.Context, cfg configuration.ConfigReader) (ObjectStore, error) {
	projectID := cfg.GetString(configuration.ObjectGcsProjectID)
	credentialsPath := cfg.GetString(configuration.ObjectGcsCredentialsPath)

	useADC := credentialsPath == ""
	logging.GetLoggerWithContext(ctx).Info("creating GCS object store",
		zap.String("project_id", projectID),
		zap.Bool("credentials_file", credentialsPath != ""),
		zap.Bool("native_auth", useADC),
	)

	return NewGcsBackend(ctx, projectID, credentialsPath)
}
