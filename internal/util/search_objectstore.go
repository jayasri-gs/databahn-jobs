package util

import (
	"context"
	"fmt"
	"os"

	"github.com/databahn-ai/common-utils/objectstore"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
)

const searchBucketKey = "search.blob.container"

var (
	searchObjectStoreClient     objectstore.ObjectStore
	searchObjectStoreCollection string
)

// _loadSearchObjectStore creates the object store client from app config (no secret overlay).
func _loadSearchObjectStore(ctx context.Context) error {
	cfg := appConfig.GetAppConfiguration()
	store, err := objectstore.NewObjectStore(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating search object store: %w", err)
	}
	searchObjectStoreClient = store
	searchObjectStoreCollection = cfg.GetString(searchBucketKey)
	return nil
}

func _getSearchObjectStore(ctx context.Context) (objectstore.ObjectStore, string, error) {
	if searchObjectStoreClient == nil {
		if err := _loadSearchObjectStore(ctx); err != nil {
			return nil, "", err
		}
	}
	return searchObjectStoreClient, searchObjectStoreCollection, nil
}

func UploadFileToObjectStore(ctx context.Context, objectKey, filePath string) error {
	client, container, err := _getSearchObjectStore(ctx)
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
	client, container, err := _getSearchObjectStore(ctx)
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
