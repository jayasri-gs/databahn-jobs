package auditReport

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/databahn-ai/common-utils/aws"
	"io"
	"os"
	"time"
)

func uploadFileToS3(ctx context.Context, filePath string, bucketName string, objectKey string) error {

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size and content type
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	_, err = file.Read(buffer)
	if err != nil && err != io.EOF {
		return err
	}

	s3PutObject := s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(buffer),
	}
	_, err = aws.UploadFileToS3(ctx, &s3PutObject)
	if err != nil {
		return err
	}
	return nil
}
func getPresignedUrl(bucketName string, objectKey string) (string, error) {
	downloadLink, err := aws.CreatePresignedLink(bucketName, objectKey, time.Hour*168)
	if err != nil {
		return "", err
	}
	return downloadLink.URL, nil
}

func getBucketNameAndObjectKey(requestId string) (string, string) {
	//region := config.GetAppConfiguration().GetString(configuration.ReportsBucketRegion)
	//bucketNama := config.GetAppConfiguration().GetString(configuration.ReportsBucketName)

	bucketName := "galaxy-databahn-report-bucket"
	timestamp := time.Now().UTC()
	objectKey := fmt.Sprintf("reports/%d/%02d/%02d/%02d/%s.csv", timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), requestId)

	return bucketName, objectKey
}
