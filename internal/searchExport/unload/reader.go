package unload

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"go.uber.org/zap"
)

type Reader struct {
	s3Client *s3.Client
	tempDir  string
	columns  []string
}

func NewReader(awsCfg aws.Config, tempDir string) (*Reader, error) {
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	return &Reader{
		s3Client: s3.NewFromConfig(awsCfg),
		tempDir:  tempDir,
	}, nil
}

func (r *Reader) ParseManifest(ctx context.Context, manifestPath string) ([]string, error) {
	bucket, key := parseS3Path(manifestPath)
	if bucket == "" {
		return nil, fmt.Errorf("invalid manifest path: %s", manifestPath)
	}

	output, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}
	defer output.Body.Close()

	var files []string
	scanner := bufio.NewScanner(output.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			files = append(files, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	sort.Strings(files)
	logging.GetLogger().Info("Found files in manifest", zap.Int("count", len(files)))
	return files, nil
}

func (r *Reader) ListFiles(ctx context.Context, s3Prefix string) ([]string, error) {
	bucket, prefix := parseS3Path(s3Prefix)
	if bucket == "" {
		return nil, fmt.Errorf("invalid S3 path: %s", s3Prefix)
	}

	var files []string
	paginator := s3.NewListObjectsV2Paginator(r.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			key := *obj.Key
			if strings.Contains(key, "manifest") {
				continue
			}
			if obj.Size != nil && *obj.Size == 0 {
				continue
			}
			if strings.HasSuffix(key, "/") {
				continue
			}
			files = append(files, fmt.Sprintf("s3://%s/%s", bucket, key))
		}
	}

	sort.Strings(files)
	logging.GetLogger().Info("Found UNLOAD output files", zap.Int("count", len(files)))
	return files, nil
}

func (r *Reader) DownloadFile(ctx context.Context, s3Path string) (string, error) {
	bucket, key := parseS3Path(s3Path)
	if bucket == "" {
		return "", fmt.Errorf("invalid S3 path: %s", s3Path)
	}

	localPath := filepath.Join(r.tempDir, filepath.Base(key))

	output, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to download %s: %w", s3Path, err)
	}
	defer output.Body.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	written, err := io.Copy(file, output.Body)
	if err != nil {
		return "", fmt.Errorf("failed to write file: %w", err)
	}

	logging.GetLogger().Info("Downloaded Parquet file",
		zap.String("file", filepath.Base(key)),
		zap.Int64("bytes", written))

	return localPath, nil
}

func (r *Reader) StreamRows(ctx context.Context, files []string, callback func(row []interface{}) error) error {
	for i, s3Path := range files {
		logging.GetLogger().Info("Converting Parquet file to export format",
			zap.Int("fileNumber", i+1),
			zap.Int("totalFiles", len(files)))

		downloadStart := time.Now()
		localPath, err := r.DownloadFile(ctx, s3Path)
		if err != nil {
			return err
		}
		downloadDuration := time.Since(downloadStart)

		processStart := time.Now()
		processErr := r.processParquetFile(localPath, callback)
		processDuration := time.Since(processStart)
		os.Remove(localPath)

		if processErr != nil {
			return fmt.Errorf("failed to process %s: %w", s3Path, processErr)
		}

		logging.GetLogger().Info("Finished Parquet file conversion",
			zap.Int("fileNumber", i+1),
			zap.Int("totalFiles", len(files)),
			zap.Duration("downloadDuration", downloadDuration),
			zap.Duration("convertDuration", processDuration))
	}

	return nil
}

func (r *Reader) processParquetFile(localPath string, callback func(row []interface{}) error) error {
	fr, err := local.NewLocalFileReader(localPath)
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	if numRows == 0 {
		return nil
	}

	if r.columns == nil {
		schema := pr.SchemaHandler
		for _, elem := range schema.SchemaElements {
			if elem.Name == "schema" || elem.Name == "spark_schema" || elem.Name == "hive_schema" {
				continue
			}
			if elem.Type != nil {
				r.columns = append(r.columns, elem.Name)
			}
		}
	}

	batchSize := 5000
	for i := 0; i < numRows; i += batchSize {
		remaining := numRows - i
		if remaining > batchSize {
			remaining = batchSize
		}

		rows, err := pr.ReadByNumber(remaining)
		if err != nil {
			return fmt.Errorf("failed to read rows at offset %d: %w", i, err)
		}

		for _, row := range rows {
			rowData := r.extractRowValues(row)
			if err := callback(rowData); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *Reader) extractRowValues(row interface{}) []interface{} {
	result := make([]interface{}, len(r.columns))

	switch v := row.(type) {
	case map[string]interface{}:
		for i, col := range r.columns {
			result[i] = dereferenceValue(v[col])
		}
	default:
		val := reflect.ValueOf(row)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if val.Kind() == reflect.Struct {
			for i, col := range r.columns {
				field := val.FieldByName(col)
				if field.IsValid() {
					result[i] = dereferenceValue(field.Interface())
				}
			}
		}
	}

	return result
}

func dereferenceValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	return val.Interface()
}

func (r *Reader) Columns() []string {
	return r.columns
}

func (r *Reader) DeleteS3Files(ctx context.Context, files []string) {
	for _, s3Path := range files {
		bucket, key := parseS3Path(s3Path)
		if bucket == "" {
			continue
		}
		_, err := r.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			logging.GetLogger().Warn("Failed to delete temp file",
				zap.String("path", s3Path), zap.Error(err))
		}
	}
}

func parseS3Path(path string) (bucket, key string) {
	path = strings.TrimPrefix(path, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1]
}
