package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type s3Backend struct {
	client          *s3.Client
	transferManager *transfermanager.Client
}

// NewS3Backend creates an S3-backed ObjectStore.
// If accessKey and secretKey are empty, uses native auth (default credential chain:
// env vars AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, IAM role, AWS config file, etc.).
func NewS3Backend(ctx context.Context, region, endpoint, accessKey, secretKey string, forcePathStyle bool) (ObjectStore, error) {
	var cfg aws.Config
	var err error

	if accessKey != "" && secretKey != "" {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				accessKey, secretKey, "",
			)),
		)
	} else {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region))
	}
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("failed to load AWS config", zap.Error(err))
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	if endpoint != "" {
		cfg.BaseEndpoint = aws.String(endpoint)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = forcePathStyle
	})
	return &s3Backend{
		client:          client,
		transferManager: transfermanager.New(client),
	}, nil
}

// NewS3BackendWithClient creates an S3-backed ObjectStore with an existing S3 client.
func NewS3BackendWithClient(client *s3.Client) ObjectStore {
	return &s3Backend{
		client:          client,
		transferManager: transfermanager.New(client),
	}
}

func (s *s3Backend) Get(ctx context.Context, container, key string) ([]byte, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(container),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get object: %w", err)
	}
	defer output.Body.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(output.Body); err != nil {
		return nil, fmt.Errorf("s3 read body: %w", err)
	}
	return buf.Bytes(), nil
}

func (s *s3Backend) Put(ctx context.Context, container, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(container),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 put object: %w", err)
	}
	return nil
}

func (s *s3Backend) PutStream(ctx context.Context, container, key string, reader io.Reader) error {
	_, err := s.transferManager.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(container),
		Key:    aws.String(key),
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("s3 upload stream: %w", err)
	}
	return nil
}

func (s *s3Backend) Post(ctx context.Context, container, key string, data []byte) error {
	// S3 does not support native append; Post is equivalent to Put
	return s.Put(ctx, container, key, data)
}

func (s *s3Backend) Delete(ctx context.Context, container, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(container),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 delete object: %w", err)
	}
	return nil
}

func (s *s3Backend) List(ctx context.Context, container, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(container),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list objects: %w", err)
		}
		for _, obj := range page.Contents {
			info := ObjectInfo{Key: aws.ToString(obj.Key)}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			objects = append(objects, info)
		}
	}
	return objects, nil
}

func (s *s3Backend) GetPresignedURL(ctx context.Context, container, key string, expiry time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(s.client)
	result, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(container),
		Key:    aws.String(key),
	}, func(po *s3.PresignOptions) {
		po.Expires = expiry
	})
	if err != nil {
		return "", fmt.Errorf("s3 presign get object: %w", err)
	}
	return result.URL, nil
}
