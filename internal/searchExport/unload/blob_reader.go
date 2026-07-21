package unload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type BlobReader struct {
	client    *azblob.Client
	container string
	tempDir   string
	columns   []string
}

func NewBlobReader(client *azblob.Client, container, tempDir string) (*BlobReader, error) {
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	return &BlobReader{client: client, container: container, tempDir: tempDir}, nil
}

func (r *BlobReader) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	prefix = strings.TrimPrefix(prefix, "/")
	var files []string
	pager := r.client.NewListBlobsFlatPager(r.container, &azblob.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list blobs: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			name := *item.Name
			if strings.HasSuffix(name, "/") {
				continue
			}
			if item.Properties != nil && item.Properties.ContentLength != nil && *item.Properties.ContentLength == 0 {
				continue
			}
			files = append(files, name)
		}
	}
	sort.Strings(files)
	logging.GetLogger().Info("Found CETAS output files", zap.Int("count", len(files)))
	return files, nil
}

func (r *BlobReader) ParseManifest(ctx context.Context, manifestPath string) ([]string, error) {
	return nil, fmt.Errorf("synapse CETAS does not use manifest files")
}

func (r *BlobReader) StreamToUploader(
	ctx context.Context,
	uploader upload.CloudUploader,
	files []string,
	header []byte,
	format string,
	delimiter string,
	log *zap.Logger,
) (int64, int64, error) {
	return streamReadersToUploader(ctx, uploader, func(ctx context.Context, file string) (io.ReadCloser, error) {
		resp, err := r.client.DownloadStream(ctx, r.container, file, nil)
		if err != nil {
			return nil, err
		}
		return resp.Body, nil
	}, files, header, log)
}

// streamReadersToUploader streams staging files in fixed-size chunks (same pattern as S3 direct upload).
func streamReadersToUploader(
	ctx context.Context,
	uploader upload.CloudUploader,
	open func(ctx context.Context, file string) (io.ReadCloser, error),
	files []string,
	header []byte,
	log *zap.Logger,
) (int64, int64, error) {
	partNum := 1
	var parts []upload.PartInfo
	var totalBytes int64
	var totalRows int64
	var pending []byte
	rows := &lineRowCounter{}

	uploadPart := func(data []byte) error {
		part, err := uploader.UploadPart(ctx, partNum, bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return err
		}
		parts = append(parts, *part)
		totalBytes += int64(len(data))
		partNum++
		return nil
	}

	flushFullParts := func() error {
		for len(pending) >= s3MinPartSize {
			if err := uploadPart(pending[:s3MinPartSize]); err != nil {
				return err
			}
			pending = pending[s3MinPartSize:]
		}
		return nil
	}

	appendChunk := func(chunk []byte) error {
		if len(chunk) == 0 {
			return nil
		}
		pending = append(pending, chunk...)
		totalRows += rows.add(chunk)
		return flushFullParts()
	}

	for i, file := range files {
		if log != nil {
			log.Info("Streaming staging file to export",
				zap.Int("fileNumber", i+1),
				zap.Int("totalFiles", len(files)),
				zap.String("path", file))
		}
		reader, err := open(ctx, file)
		if err != nil {
			return totalRows, totalBytes, err
		}
		if i == 0 && len(header) > 0 {
			pending = append(pending, header...)
			if err := flushFullParts(); err != nil {
				reader.Close()
				return totalRows, totalBytes, err
			}
		}
		buf := make([]byte, streamChunkSize)
		for {
			n, readErr := reader.Read(buf)
			if n > 0 {
				if err := appendChunk(buf[:n]); err != nil {
					reader.Close()
					return totalRows, totalBytes, err
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				reader.Close()
				return totalRows, totalBytes, readErr
			}
		}
		if err := reader.Close(); err != nil {
			return totalRows, totalBytes, err
		}
	}

	totalRows += rows.finish()

	if len(pending) > 0 {
		if err := uploadPart(pending); err != nil {
			return totalRows, totalBytes, err
		}
	}
	if len(parts) == 0 {
		return 0, 0, nil
	}
	if err := uploader.Complete(ctx, parts); err != nil {
		return totalRows, totalBytes, err
	}
	return totalRows, totalBytes, nil
}

func (r *BlobReader) StreamRows(ctx context.Context, files []string, callback func(row []interface{}) error) error {
	for i, blobName := range files {
		if err := func() error {
			localPath := filepath.Join(r.tempDir, filepath.Base(blobName))
			defer os.Remove(localPath)

			resp, err := r.client.DownloadStream(ctx, r.container, blobName, nil)
			if err != nil {
				return fmt.Errorf("download %s: %w", blobName, err)
			}
			f, err := os.Create(localPath)
			if err != nil {
				resp.Body.Close()
				return err
			}
			if _, err := io.Copy(f, resp.Body); err != nil {
				f.Close()
				resp.Body.Close()
				return err
			}
			f.Close()
			resp.Body.Close()
			if err := ProcessLocalParquetFile(localPath, &r.columns, callback); err != nil {
				return fmt.Errorf("process %s: %w", blobName, err)
			}
			logging.GetLogger().Info("Finished blob parquet conversion", zap.Int("fileNumber", i+1))
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (r *BlobReader) DeleteFiles(ctx context.Context, files []string) error {
	for _, name := range files {
		_, err := r.client.DeleteBlob(ctx, r.container, name, nil)
		if err != nil {
			logging.GetLogger().Warn("failed to delete staging blob", zap.String("blob", name), zap.Error(err))
		}
	}
	return nil
}

func (r *BlobReader) Columns() []string { return r.columns }
