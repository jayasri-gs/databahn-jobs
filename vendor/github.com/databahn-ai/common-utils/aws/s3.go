package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
	"io"
	"time"
)

func getS3Client() (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	return s3.NewFromConfig(cfg), err
}

func DownloadFileFromS3(path string, bucketName string) (text []byte, err error) {
	client, err := getS3Client()
	logging.Info("downloading s3 file", zap.String("bucket", bucketName))
	if err != nil {
		logging.Error("error while starting s3 client", zap.Error(err))
		return nil, err
	}

	getFile := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}
	ctx := context.Background()

	content, err := client.GetObject(ctx, getFile)

	if err != nil {
		logging.Error("error while pulling data.", zap.Error(err))
		return nil, err
	}
	body, err := io.ReadAll(content.Body)
	if err != nil {
		logging.Error("error while fetching file contents", zap.Error(err))
	}
	return body, nil
}

func UploadFileToS3(ctx context.Context, request *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	client, err := getS3Client()
	logging.Info("uploading s3 file", zap.Any("bucket", request.Bucket))
	if err != nil {
		logging.Error("error while starting s3 client", zap.Error(err))
		return nil, err
	}

	return client.PutObject(ctx, request)
}

// CreatePresignedLink will return presigned url for given s3 object
func CreatePresignedLink(bucketName string, path string) (*v4.PresignedHTTPRequest, error) {

	client, err := getS3Client()
	logging.Info("bucket name " + bucketName)
	if err != nil {
		logging.Error("error while starting s3 client", zap.Error(err))
		return nil, err
	}

	presignClient := s3.NewPresignClient(client)

	presignParams := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}

	presignDuration := func(po *s3.PresignOptions) {
		po.Expires = 5 * time.Minute
	}

	presignResult, err := presignClient.PresignGetObject(context.TODO(), presignParams, presignDuration)

	if err != nil {
		fmt.Printf("Couldn't get presigned URL for GetObject %v\n", err)
	}

	fmt.Printf("Presigned URL For object: %s\n", presignResult.URL)
	return presignResult, err
}
