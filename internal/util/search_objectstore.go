package util

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/objectstore"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
)

const (
	searchBucketKey          = "search.blob.container"
	searchBlobAccountNameKey = "search.blob.account_name"
)

var (
	searchObjectStoreClient     objectstore.ObjectStore
	searchObjectStoreCollection string
)

type AwsSearchSecret struct {
	AccessKeyID     string `json:"search.access_key_id"`
	SecretAccessKey string `json:"search.secret_access_key"`
	Bucket          string `json:"search.bucket"`
}

func loadS3Secret(_ context.Context) (*AwsSearchSecret, error) {
	secretName := appConfig.GetAppConfiguration().GetString("search.secret_name")
	region := appConfig.GetAppConfiguration().GetString("region")
	data, err := aws.ReadSecretByName(secretName, region)
	if err != nil {
		return nil, err
	}
	awsSecret := &AwsSearchSecret{}
	err = json.Unmarshal([]byte(*data.SecretString), &awsSecret)
	if err != nil {
		return nil, err
	}
	return awsSecret, nil
}

// loadSearchObjectStore creates the object store client from app config (no secret overlay).
func loadSearchObjectStore(ctx context.Context) error {
	cfg := appConfig.GetAppConfiguration()
	backend := cfg.GetString(configuration.ObjectBackend)

	if backend == objectstore.BackendS3 {
		awsSecret, err := loadS3Secret(ctx)
		if err != nil {
			return fmt.Errorf("loading S3 secret: %w", err)
		}
		region := cfg.GetString(configuration.ObjectS3Region)
		if region == "" {
			region = os.Getenv("AWS_REGION")
		}
		if region == "" {
			region = os.Getenv("AWS_DEFAULT_REGION")
		}
		store, err := objectstore.NewS3Backend(ctx, region, cfg.GetString(configuration.ObjectS3Endpoint), awsSecret.AccessKeyID, awsSecret.SecretAccessKey, cfg.GetBool(configuration.ObjectS3ForcePathStyle))
		if err != nil {
			return fmt.Errorf("creating S3 search object store: %w", err)
		}
		searchObjectStoreClient = store
		searchObjectStoreCollection = awsSecret.Bucket
		return nil
	}

	if backend == objectstore.BackendBlob {
		accountName := cfg.GetString(searchBlobAccountNameKey)
		container := cfg.GetString(searchBucketKey)

		store, err := objectstore.NewBlobBackend(ctx, "", accountName, "")
		if err != nil {
			return fmt.Errorf("creating blob search object store: %w", err)
		}
		searchObjectStoreClient = store
		searchObjectStoreCollection = container

		return nil
	}
	return nil
}

func getSearchObjectStore(ctx context.Context) (objectstore.ObjectStore, string, error) {
	if searchObjectStoreClient == nil {
		if err := loadSearchObjectStore(ctx); err != nil {
			return nil, "", err
		}
	}
	return searchObjectStoreClient, searchObjectStoreCollection, nil
}

func UploadFileToObjectStore(ctx context.Context, objectKey, filePath string) error {
	client, container, err := getSearchObjectStore(ctx)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	return client.Put(ctx, container, objectKey, data)
}

func UploadGzipFileToObjectStore(ctx context.Context, objectKey, filePath string) error {
	client, container, err := getSearchObjectStore(ctx)
	if err != nil {
		return err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	return client.PutStream(ctx, container, objectKey, file, &objectstore.PutOptions{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
	})
}
