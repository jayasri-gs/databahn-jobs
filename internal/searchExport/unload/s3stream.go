package unload

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
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
	format string,
	delimiter string,
	log *zap.Logger,
) (int64, int64, error) {
	partNum := 1
	var parts []upload.PartInfo
	var totalBytes int64
	var totalRows int64

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

		output, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return totalRows, totalBytes, fmt.Errorf("failed to get %s: %w", s3Path, err)
		}

		var fileBuf bytes.Buffer
		body := io.TeeReader(output.Body, &fileBuf)
		size := int64(0)
		if output.ContentLength != nil {
			size = *output.ContentLength
		}

		// Prepend header to the first part so we don't upload a sub-5MB part (S3 multipart minimum).
		partReader := io.Reader(body)
		if i == 0 && len(header) > 0 {
			partReader = io.MultiReader(bytes.NewReader(header), body)
			size += int64(len(header))
		}

		part, err := uploader.UploadPart(ctx, partNum, partReader, size)
		output.Body.Close()
		if err != nil {
			return totalRows, totalBytes, fmt.Errorf("failed to upload part %d: %w", partNum, err)
		}
		parts = append(parts, *part)
		totalBytes += size
		totalRows += countRecords(fileBuf.Bytes(), format, delimiter)
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

func countRecords(data []byte, format, delimiter string) int64 {
	if len(data) == 0 {
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		dec := json.NewDecoder(bytes.NewReader(data))
		var n int64
		for dec.More() {
			var row json.RawMessage
			if err := dec.Decode(&row); err != nil {
				break
			}
			n++
		}
		return n
	default:
		delim := ','
		if delimiter != "" {
			if r, size := utf8.DecodeRuneInString(delimiter); size > 0 && r != utf8.RuneError {
				delim = r
			}
		}
		reader := csv.NewReader(bytes.NewReader(data))
		reader.Comma = delim
		reader.ReuseRecord = true
		var n int64
		for {
			if _, err := reader.Read(); err == io.EOF {
				return n
			} else if err != nil {
				return n
			}
			n++
		}
	}
}
