package unload

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"go.uber.org/zap"
)

// StreamToUploader copies UNLOAD output files from S3 directly into a multipart upload,
// avoiding local disk and Parquet conversion.
func (r *Reader) StreamToUploader(
	ctx context.Context,
	uploader upload.CloudUploader,
	files []string,
	header []byte,
	log *zap.Logger,
) (int64, int64, error) {
	partNum := 1
	var parts []upload.PartInfo
	var totalBytes int64
	var totalRows int64

	uploadPart := func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		size := int64(len(data))
		part, err := uploader.UploadPart(ctx, partNum, bytes.NewReader(data), size)
		if err != nil {
			return fmt.Errorf("failed to upload part %d: %w", partNum, err)
		}
		parts = append(parts, *part)
		totalBytes += size
		partNum++
		return nil
	}

	if len(header) > 0 {
		if err := uploadPart(header); err != nil {
			return 0, 0, err
		}
	}

	for i, s3Path := range files {
		if log != nil {
			log.Info("Streaming UNLOAD file to export",
				zap.Int("fileNumber", i+1),
				zap.Int("totalFiles", len(files)),
				zap.String("s3Path", s3Path))
		}

		bucket, key := parseS3Path(s3Path)
		if bucket == "" {
			return totalRows, totalBytes, fmt.Errorf("invalid S3 path: %s", s3Path)
		}

		head, err := r.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return totalRows, totalBytes, fmt.Errorf("failed to head %s: %w", s3Path, err)
		}

		output, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return totalRows, totalBytes, fmt.Errorf("failed to get %s: %w", s3Path, err)
		}

		var counter lineCounter
		body := io.TeeReader(output.Body, &counter)
		size := int64(0)
		if head.ContentLength != nil {
			size = *head.ContentLength
		}

		part, err := uploader.UploadPart(ctx, partNum, body, size)
		output.Body.Close()
		if err != nil {
			return totalRows, totalBytes, fmt.Errorf("failed to upload part %d: %w", partNum, err)
		}
		parts = append(parts, *part)
		totalBytes += size
		totalRows += counter.lines
		partNum++
	}

	if len(parts) == 0 {
		return 0, 0, nil
	}

	if err := uploader.Complete(ctx, parts); err != nil {
		return totalRows, totalBytes, fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return totalRows, totalBytes, nil
}

// BuildCSVHeader returns a CSV header line for the given columns and delimiter.
func BuildCSVHeader(columns []string, delimiter string) []byte {
	delimRune := ','
	if r, size := utf8.DecodeRuneInString(delimiter); size > 0 && r != utf8.RuneError {
		delimRune = r
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Comma = delimRune
	_ = w.Write(columns)
	w.Flush()
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] != '\n' {
		data = append(data, '\n')
	}
	return data
}

type lineCounter struct {
	lines int64
}

func (c *lineCounter) Write(p []byte) (int, error) {
	c.lines += int64(bytes.Count(p, []byte{'\n'}))
	return len(p), nil
}
