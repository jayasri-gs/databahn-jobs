package dbaws

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/replay/replaymanager"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func downloadFileFromAzureBlob(input model.Message, fileName string, mst *replaymanager.MetaDataStore, threadId int) error {

	logger.GetLogger().Info("download Started.")
	metaMap, ok := mst.GetMetaData(fileName)
	if !ok {
		return fmt.Errorf("metadata not found for file %s", fileName)
	}
	fullPath := filepath.Join(metaMap.Prefix, fileName)

	objectKey := fullPath
	bucketName := input.BucketName

	logger.GetLogger().Info("blob params", zap.String("azure_blob_container", input.AdditionalConfig["azure_blob_container"]), zap.String("fullPath", fullPath), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))

	containerName := input.AdditionalConfig["azure_blob_container"]

	client, err := GetAzureBlobClient(input.AdditionalConfig)
	if err != nil {
		logger.GetLogger().Error("couldn't create Azure Blob client", zap.Error(err))
		return err
	}

	path := mst.GetPath("dataDir")
	logger.GetLogger().Info("mount location.", zap.String("location", path), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
	err = replaymanager.CreateDirIfNotExist(path)
	path = filepath.Join(path, fileName)
	if err != nil {
		logger.GetLogger().Error("couldn't create Dir Here's why  ", zap.String("key", bucketName), zap.String("key", objectKey), zap.String("error", err.Error()), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		logger.GetLogger().Error("couldn't Create file to Dir Here's why  ", zap.String("key", bucketName), zap.String("key", objectKey), zap.String("error", err.Error()), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
		return err
	}
	// remember to close the file
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			logger.GetLogger().Error(err.Error())
		}
	}(f)

	stream, err := client.DownloadStream(context.Background(), containerName, objectKey, &azblob.DownloadStreamOptions{})
	if err != nil {
		logger.GetLogger().Error(err.Error())
		return err
	}
	body := stream.Body
	defer body.Close()

	// Write to file
	_, err = io.Copy(f, body)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	logger.GetLogger().Info("download completed.", zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
	return nil
}
