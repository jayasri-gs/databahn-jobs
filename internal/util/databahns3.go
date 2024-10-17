package util

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/databahn-ai/common-utils/aws"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"io"
	"os"
)

type AwsSearchSecret struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Bucket          string `json:"bucket"`
}

var s3Client *s3.Client
var awsSecret *AwsSearchSecret

func _loadS3ClientAndSecret(ctx context.Context) error {
	secretName := appConfig.GetAppConfiguration().GetString("search.secret_name")
	region := appConfig.GetAppConfiguration().GetString("region")
	data, err := aws.ReadSecretByName(secretName, region)
	if err != nil {
		return err
	}
	awsSecret = &AwsSearchSecret{}
	err = json.Unmarshal([]byte(*data.SecretString), &awsSecret)
	if err != nil {
		return err
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsSecret.AccessKeyID, awsSecret.SecretAccessKey, "")),
	)
	if err != nil {
		return err
	}
	s3Client = s3.NewFromConfig(cfg)
	return nil
}

func _getSearchS3Client(ctx context.Context) (*s3.Client, error) {
	if s3Client == nil {
		err := _loadS3ClientAndSecret(ctx)
		if err != nil {
			return nil, err
		}
	}
	return s3Client, nil
}

func UploadFileToS3(ctx context.Context, objectKey, filePath string) error {
	client, err := _getSearchS3Client(ctx)
	if err != nil {
		return err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	buffer, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	s3PutObject := s3.PutObjectInput{
		Bucket: aws.String(awsSecret.Bucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(buffer),
	}
	_, err = client.PutObject(ctx, &s3PutObject)
	if err != nil {
		return err
	}
	return file.Close()
}

func UploadGzipFileToS3(ctx context.Context, objectKey, filePath string) error {
	client, err := _getSearchS3Client(ctx)
	if err != nil {
		return err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	buffer, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	s3PutObject := s3.PutObjectInput{
		Bucket:          aws.String(awsSecret.Bucket),
		Key:             aws.String(objectKey),
		Body:            bytes.NewReader(buffer),
		ContentEncoding: aws.String("gzip"),
		ContentType:     aws.String("text/plain"),
	}
	_, err = client.PutObject(ctx, &s3PutObject)
	if err != nil {
		return err
	}
	return file.Close()
}
