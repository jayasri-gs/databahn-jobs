package common

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/auditReport/consts"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/objstore"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func UploadFileToObjectStoreAndUpdateInDb(ctx context.Context, file *os.File, req models.AuditReport, bucketName string, objectKey string) error {
	// upload the file to s3
	err := uploadFile(ctx, file.Name(), bucketName, objectKey)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while uploading file to s3", zap.Error(err))
		return err
	}

	// get pre-signed link for the uploaded file
	downloadLink, err := getPresignedUrl(bucketName, objectKey)
	err = os.Remove(file.Name())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while deleting temp file", zap.Error(err))
		return err
	}

	err = models.UpdateRequestStatusAndDownloadLink(config.GetDB(), req.Id.String(), consts.COMPLETED, downloadLink, time.Now().Add(time.Hour*12))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while updating status to completed", zap.Error(err))
		return err
	}
	return err
}
func uploadFile(ctx context.Context, filePath string, bucketName string, objectKey string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	return objstore.GetClient().PutStream(ctx, bucketName, objectKey, file)
}
func getPresignedUrl(bucketName string, objectKey string) (string, error) {
	downloadLink, err := objstore.GetClient().GetPresignedURL(context.Background(), bucketName, objectKey, time.Hour*168)
	if err != nil {
		return "", err
	}
	return downloadLink, nil
}

func GetBucketNameAndObjectKey(requestId string) (string, string) {
	bucketName := objstore.GetBucket(objstore.BucketArtifacts)
	timestamp := time.Now().UTC()
	objectKey := fmt.Sprintf("audit-reports/%d/%02d/%02d/%02d/%s.csv", timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), requestId)

	return bucketName, objectKey
}
