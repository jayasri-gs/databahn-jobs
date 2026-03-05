package dbaws

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/constants"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/replaymanager"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/objstore"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func downloadFile(ctx context.Context, input model.Message, fileName string, mst *replaymanager.MetaDataStore, threadId int) error {
	metaMap := mst.GetMetaMap()[fileName]
	fullPath := filepath.Join(metaMap.Prefix, fileName)
	bucketName := input.BucketName

	logger.GetLogger().Info("download started",
		zap.String("bucket", bucketName),
		zap.String("fullPath", fullPath),
		zap.String("traceId", input.RequestId),
		zap.Int("thread", threadId))

	data, err := objstore.GetClient().Get(ctx, bucketName, fullPath)
	if err != nil {
		logger.GetLogger().Error("couldn't download object",
			zap.String("bucket", bucketName),
			zap.String("key", fullPath),
			zap.Error(err),
			zap.String("traceId", input.RequestId),
			zap.Int("thread", threadId))
		return err
	}

	dir := mst.GetPath("dataDir")
	logger.GetLogger().Info("mount location",
		zap.String("location", dir),
		zap.String("traceId", input.RequestId),
		zap.Int("thread", threadId))

	if err = replaymanager.CreateDirIfNotExist(dir); err != nil {
		logger.GetLogger().Error("couldn't create dir",
			zap.String("bucket", bucketName),
			zap.String("key", fullPath),
			zap.Error(err),
			zap.String("traceId", input.RequestId),
			zap.Int("thread", threadId))
		return err
	}

	filePath := filepath.Join(dir, fileName)
	if err = os.WriteFile(filePath, data, 0644); err != nil {
		logger.GetLogger().Error("couldn't write file",
			zap.String("bucket", bucketName),
			zap.String("key", fullPath),
			zap.Error(err),
			zap.String("traceId", input.RequestId),
			zap.Int("thread", threadId))
		return err
	}

	logger.GetLogger().Info("download completed",
		zap.String("traceId", input.RequestId),
		zap.Int("thread", threadId))
	return nil
}

func ObjectStoreFileDownloader(input model.Message, threadId int, mst *replaymanager.MetaDataStore, fileName string, metaValue model.MetaDataValue) (error, string) {
	if metaValue.Retry >= constants.MaxRetry {
		logger.GetLogger().Info(fmt.Sprintf("max retries exceeded skipping file {%s}", fileName),
			zap.String("traceId", input.RequestId),
			zap.Int("thread", threadId))
		return errors.New("max retries exceeded"), ""
	}
	if metaValue.Status != constants.StatusDownloaded {
		logger.GetLogger().Info(fmt.Sprintf("file is not downloaded trying to download {%s}", fileName),
			zap.String("traceId", input.RequestId),
			zap.Int("thread", threadId))

		err := downloadFile(context.TODO(), input, fileName, mst, threadId)
		if err != nil {
			logger.GetLogger().Error("error while downloading file",
				zap.Error(err),
				zap.String("filename", fileName),
				zap.String("traceId", input.RequestId),
				zap.Int("thread", threadId))
			return err, constants.StatusDownloadFailed
		}
		mst.UpdateMetaData(fileName, constants.StatusDownloaded, 0, 0, 0, 0, "")

		logger.GetLogger().Info("download is completed",
			zap.String("traceId", input.RequestId),
			zap.Int("thread", threadId))
	} else {
		logger.GetLogger().Info("no need to download file, already found in system",
			zap.String("traceId", input.RequestId),
			zap.Int("thread", threadId))
	}

	return nil, ""
}

func DeleteFileFromObjectStore(input model.Message, mst *replaymanager.MetaDataStore, fileName string) error {
	metaObject := mst.GetMetaMap()[fileName]
	fullPath := metaObject.Prefix + "/" + metaObject.FileName

	err := objstore.GetClient().Delete(context.TODO(), input.BucketName, fullPath)
	if err != nil {
		logger.GetLogger().Debug("delete failed",
			zap.String("bucketName", input.BucketName),
			zap.String("fullFilePath", fullPath),
			zap.String("traceId", input.RequestId))
		return err
	}

	logger.GetLogger().Info("deleted file from object store",
		zap.String("bucketName", input.BucketName),
		zap.String("fullFilePath", fullPath))
	return nil
}
