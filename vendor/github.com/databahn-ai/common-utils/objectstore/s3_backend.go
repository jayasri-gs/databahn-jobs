package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func (s *s3Backend) Put(ctx context.Context, container, key string, data []byte, opts ...*PutOptions) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(container),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	if len(opts) > 0 && opts[0] != nil {
		if opts[0].ContentType != "" {
			input.ContentType = aws.String(opts[0].ContentType)
		}
		if opts[0].ContentEncoding != "" {
			input.ContentEncoding = aws.String(opts[0].ContentEncoding)
		}
	}
	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 put object: %w", err)
	}
	return nil
}

func (s *s3Backend) PutStream(ctx context.Context, container, key string, reader io.Reader, opts ...*PutOptions) error {
	input := &transfermanager.UploadObjectInput{
		Bucket: aws.String(container),
		Key:    aws.String(key),
		Body:   reader,
	}
	if len(opts) > 0 && opts[0] != nil {
		if opts[0].ContentType != "" {
			input.ContentType = aws.String(opts[0].ContentType)
		}
		if opts[0].ContentEncoding != "" {
			input.ContentEncoding = aws.String(opts[0].ContentEncoding)
		}
	}
	_, err := s.transferManager.UploadObject(ctx, input)
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

func (s *s3Backend) DeleteBatch(ctx context.Context, container string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	const maxBatchSize = 1000
	for i := 0; i < len(keys); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		identifiers := make([]types.ObjectIdentifier, len(batch))
		for j, key := range batch {
			identifiers[j] = types.ObjectIdentifier{Key: aws.String(key)}
		}

		output, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(container),
			Delete: &types.Delete{
				Objects: identifiers,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("s3 batch delete: %w", err)
		}

		if len(output.Errors) > 0 {
			msgs := make([]string, 0, len(output.Errors))
			for _, e := range output.Errors {
				msgs = append(msgs, fmt.Sprintf("key=%s: %s", aws.ToString(e.Key), aws.ToString(e.Message)))
			}
			return fmt.Errorf("s3 batch delete partial failure: %s", strings.Join(msgs, "; "))
		}
	}
	return nil
}
