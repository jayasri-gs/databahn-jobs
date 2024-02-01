package dbaws

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/ecryption"
	"github.com/databahn-ai/databahn-jobs/internal/replay/lookup"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/replay/replaymanager"
	"github.com/databahn-ai/databahn-jobs/internal/replay/utils"

	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	utilsc "github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"path/filepath"
)

func createAwsConnection(input model.Message) (*s3.Client, error) {

	traceId := input.RequestId
	key, secret := ecryption.DecryptKeys(input.AccessKeyID, input.SecretAccessKey, "")
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		// Hard coded credentials
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{

			Value: aws.Credentials{
				AccessKeyID: key, SecretAccessKey: secret, SessionToken: "",
				Source: "from kafka topic",
			},
		}), config.WithRegion(input.Region))
	if err != nil {
		logger.GetLogger().Info("failed to load Config", zap.Error(err), zap.String("raceId", traceId))
		return nil, err
	}
	client := s3.NewFromConfig(cfg)
	return client, nil

}

func getOrCreateS3Connection(input model.Message) (*s3.Client, error) {

	var s3Client *s3.Client
	traceId := input.RequestId
	if val, exists := lookup.Cache.Get(input.AccessKeyID, traceId); exists {
		logger.GetLogger().Info("cache hit: ", zap.String("key", utils.ReplaceChars(input.AccessKeyID)), zap.String("raceId", traceId))
		s3Client = val.(*s3.Client)
	} else {
		logger.GetLogger().Info("cache miss: ", zap.String("key", utils.ReplaceChars(input.AccessKeyID)), zap.String("raceId", traceId))
		var er error
		s3Client, er = createAwsConnection(input)
		if er != nil {
			logger.GetLogger().Error("error: using createAwsConnection", zap.Error(er), zap.String("raceId", traceId))
			return nil, er
		}
		lookup.Cache.Set(input.AccessKeyID, s3Client, traceId)
	}
	return s3Client, nil
}

func downloadFileFromS3(s3Client *s3.Client, input model.Message, fileName string, mst *replaymanager.MetaDataStore, threadId int) error {

	logger.GetLogger().Info("download Started.")
	fullPath := filepath.Join(input.BucketPrefix, fileName)

	objectKey := fullPath
	bucketName := input.BucketName
	logger.GetLogger().Info("s3 params", zap.String("key", bucketName), zap.String("fullPath", fullPath), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
	var partMiBs int64 = 30

	downloader := manager.NewDownloader(s3Client, func(d *manager.Downloader) {
		d.PartSize = partMiBs * 1024 * 1024
		d.Concurrency = utilsc.GetEnvInt("PROCESSED_CONSUMER_NO_OF_THREADS", constants.S3FileDownloadConcurrency)

	})

	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(context.TODO(), buffer, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		logger.GetLogger().Error("couldn't download large object here's why", zap.String("key", bucketName), zap.String("key", objectKey), zap.String("error", err.Error()), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
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

	// write bytes to the file
	_, err = f.Write(buffer.Bytes())
	if err != nil {
		logger.GetLogger().Error("unable to write in to buffer of file   ", zap.String("key", bucketName), zap.String("key", objectKey), zap.String("error", err.Error()), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
		return err
	}

	logger.GetLogger().Info("download completed.", zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
	return nil
}

//DownloadLargeObject uses a download manager to download an object from a bucket.
//The download manager gets the data in parts and writes them to a buffer until complete
//the data has been downloaded.

func S3FileDownloader(input model.Message, threadId int, mst *replaymanager.MetaDataStore, fileName string, metaValue model.MetaDataValue) (error, string) {

	if metaValue.Retry >= constants.MaxRetry {
		logger.GetLogger().Info(fmt.Sprintf("max retries exceeded skipping file {%d}", threadId), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
		return errors.New("max retries exceeded"), ""
	}
	if metaValue.Status != constants.StatusDownloaded {
		logger.GetLogger().Info(fmt.Sprintf("file is not downloaded trying to download {%d}", threadId), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))

		s3Client, er := getOrCreateS3Connection(input)
		if er != nil {
			logger.GetLogger().Error("error: while creating aws session  ", zap.Error(er), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
			return er, constants.StatusDownloadFailed
		}

		err := downloadFileFromS3(s3Client, input, fileName, mst, threadId)
		if err != nil {
			logger.GetLogger().Error(" error while downloading file from s3  ", zap.Error(err), zap.String("filename", fileName), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
			return err, constants.StatusDownloadFailed
		}
		mst.UpdateMetaData(fileName, constants.StatusDownloaded, 0, 0, 0, 0, "")

		logger.GetLogger().Info(fmt.Sprintf("download is completed"), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
	} else {
		logger.GetLogger().Info(fmt.Sprintf("no need to downloaded file, already found in system"), zap.String("traceId", input.RequestId), zap.Int("thread ", threadId))
	}

	return nil, ""
}
