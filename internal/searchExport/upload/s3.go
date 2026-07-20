package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type S3Uploader struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
	key           string
	uploadID      string
	contentType   string
	lifecycleTag  string
}

func NewS3Uploader(awsCfg aws.Config, lifecycleTag string) *S3Uploader {
	client := s3.NewFromConfig(awsCfg)
	return &S3Uploader{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		lifecycleTag:  lifecycleTag,
	}
}

func (u *S3Uploader) UploadID() string { return u.uploadID }

func (u *S3Uploader) Init(ctx context.Context, bucket, key, contentType string) error {
	u.bucket = bucket
	u.key = key
	u.contentType = contentType

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}
	if u.lifecycleTag != "" {
		input.Tagging = aws.String(u.lifecycleTag)
	}

	output, err := u.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	u.uploadID = *output.UploadId
	logging.GetLogger().Info("Started S3 multipart upload",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("uploadId", u.uploadID))

	return nil
}

func (u *S3Uploader) UploadPart(ctx context.Context, partNumber int, data io.Reader, size int64) (*PartInfo, error) {
	output, err := u.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(u.bucket),
		Key:           aws.String(u.key),
		UploadId:      aws.String(u.uploadID),
		PartNumber:    aws.Int32(int32(partNumber)),
		Body:          data,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upload part %d: %w", partNumber, err)
	}

	logging.GetLogger().Debug("Uploaded part",
		zap.Int("partNumber", partNumber),
		zap.Int64("size", size))

	return &PartInfo{
		PartNumber: partNumber,
		ETag:       *output.ETag,
		Size:       size,
	}, nil
}

func (u *S3Uploader) Complete(ctx context.Context, parts []PartInfo) error {
	completedParts := make([]s3types.CompletedPart, len(parts))
	for i, p := range parts {
		completedParts[i] = s3types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(int32(p.PartNumber)),
		}
	}

	_, err := u.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	logging.GetLogger().Info("Completed S3 multipart upload",
		zap.String("bucket", u.bucket),
		zap.String("key", u.key),
		zap.Int("parts", len(parts)))

	return nil
}

func (u *S3Uploader) Abort(ctx context.Context) error {
	if u.uploadID == "" {
		return nil
	}

	_, err := u.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
	})
	return err
}

// multipartAPI is the subset of the S3 client used for orphan cleanup, extracted
// so cleanup can be tested without a real client.
type multipartAPI interface {
	ListMultipartUploads(ctx context.Context, params *s3.ListMultipartUploadsInput, optFns ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// AbortIncompleteUploads aborts every in-progress multipart upload under keyPrefix.
// Used to clean up uploads orphaned by a crashed previous attempt (no checkpoint
// records the upload ID). Best-effort: failures are logged, never returned.
func (u *S3Uploader) AbortIncompleteUploads(ctx context.Context, bucket, keyPrefix string) {
	if u.client == nil || bucket == "" || keyPrefix == "" {
		return
	}
	abortIncompleteUploads(ctx, u.client, bucket, keyPrefix)
}

func abortIncompleteUploads(ctx context.Context, client multipartAPI, bucket, keyPrefix string) {
	log := logging.GetLogger()
	input := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(keyPrefix),
	}
	for {
		page, err := client.ListMultipartUploads(ctx, input)
		if err != nil {
			log.Warn("Failed to list incomplete multipart uploads",
				zap.String("bucket", bucket),
				zap.String("prefix", keyPrefix),
				zap.Error(err))
			return
		}
		for _, up := range page.Uploads {
			if _, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      up.Key,
				UploadId: up.UploadId,
			}); err != nil {
				var nsu *s3types.NoSuchUpload
				if !errors.As(err, &nsu) {
					log.Warn("Failed to abort incomplete multipart upload",
						zap.String("key", aws.ToString(up.Key)),
						zap.Error(err))
				}
				continue
			}
			log.Info("Aborted orphaned multipart upload",
				zap.String("bucket", bucket),
				zap.String("key", aws.ToString(up.Key)),
				zap.String("uploadId", aws.ToString(up.UploadId)))
		}
		if !aws.ToBool(page.IsTruncated) {
			return
		}
		input.KeyMarker = page.NextKeyMarker
		input.UploadIdMarker = page.NextUploadIdMarker
	}
}

func (u *S3Uploader) GeneratePresignedURL(ctx context.Context, expiry time.Duration) (string, error) {
	filename := filepath.Base(u.key)
	output, err := u.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket:                     aws.String(u.bucket),
		Key:                        aws.String(u.key),
		ResponseContentDisposition: aws.String(fmt.Sprintf(`attachment; filename="%s"`, filename)),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}
	return output.URL, nil
}

func (u *S3Uploader) GetLocation() string {
	return fmt.Sprintf("s3://%s/%s", u.bucket, u.key)
}
