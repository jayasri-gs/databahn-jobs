package unload

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"go.uber.org/zap"
)

// S3 requires every multipart part except the last to be at least 5 MiB.
const (
	s3MinPartSize   = 5 * 1024 * 1024
	streamChunkSize = 256 * 1024
)

// StreamOptions configures resume behaviour and checkpoint callbacks for StreamToUploader.
type StreamOptions struct {
	// StartFileIndex skips files 0..StartFileIndex-1 (already uploaded on a previous run).
	StartFileIndex int
	// StartPartNumber is the next part number to use. Set to lastUploadedPart+1 on resume.
	StartPartNumber int
	// ExistingParts are already-uploaded parts recovered via ListParts; prepended to the
	// parts slice passed to Complete so the final multipart upload is contiguous.
	ExistingParts []upload.PartInfo
	// OnFileCheckpoint is called after a file is fully streamed AND pending is empty
	// (all bytes committed as complete S3 parts). Safe to write EFS checkpoint here.
	OnFileCheckpoint func(fileIndex, partNum int, rows, bytes int64)
}

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
	opts StreamOptions,
) (int64, int64, error) {
	partNum := 1
	if opts.StartPartNumber > 1 {
		partNum = opts.StartPartNumber
	}

	parts := make([]upload.PartInfo, len(opts.ExistingParts))
	copy(parts, opts.ExistingParts)

	var totalBytes int64
	var totalRows int64
	var pending []byte
	rows := &lineRowCounter{}

	uploadPart := func(data []byte) error {
		part, err := uploader.UploadPart(ctx, partNum, bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return fmt.Errorf("failed to upload part %d: %w", partNum, err)
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

	for i, s3Path := range files {
		if i < opts.StartFileIndex {
			continue
		}

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

		reader, err := openUnloadReader(output.Body, key)
		if err != nil {
			output.Body.Close()
			return totalRows, totalBytes, fmt.Errorf("failed to open %s: %w", s3Path, err)
		}

		// Prepend header before first file's data (only on fresh run, not resume)
		if i == opts.StartFileIndex && i == 0 && len(header) > 0 {
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
				return totalRows, totalBytes, fmt.Errorf("failed to read %s: %w", s3Path, readErr)
			}
		}
		if err := reader.Close(); err != nil {
			return totalRows, totalBytes, fmt.Errorf("failed to close %s: %w", s3Path, err)
		}

		// Checkpoint only when pending is empty — all bytes committed as complete S3 parts.
		if len(pending) == 0 && opts.OnFileCheckpoint != nil {
			opts.OnFileCheckpoint(i+1, partNum-1, totalRows, totalBytes)
		}
	}

	totalRows += rows.finish()

	if len(pending) > 0 {
		if err := uploadPart(pending); err != nil {
			return totalRows, totalBytes, err
		}
		pending = nil
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

func openUnloadReader(body io.ReadCloser, key string) (io.ReadCloser, error) {
	if !strings.HasSuffix(strings.ToLower(key), ".gz") {
		return body, nil
	}
	gr, err := gzip.NewReader(body)
	if err != nil {
		body.Close()
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	return &gzipReadCloser{Reader: gr, closers: []io.Closer{gr, body}}, nil
}

type gzipReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (g *gzipReadCloser) Close() error {
	var err error
	for _, c := range g.closers {
		if e := c.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// lineRowCounter counts newline-delimited records across chunk boundaries (Athena UNLOAD output).
type lineRowCounter struct {
	tail []byte
}

func (c *lineRowCounter) add(chunk []byte) int64 {
	data := append(c.tail, chunk...)
	c.tail = nil
	lastNL := bytes.LastIndexByte(data, '\n')
	if lastNL < 0 {
		c.tail = data
		return 0
	}
	if lastNL < len(data)-1 {
		c.tail = append(c.tail, data[lastNL+1:]...)
	}
	return int64(bytes.Count(data[:lastNL+1], []byte{'\n'}))
}

func (c *lineRowCounter) finish() int64 {
	if len(c.tail) == 0 {
		return 0
	}
	n := int64(1)
	c.tail = nil
	return n
}
