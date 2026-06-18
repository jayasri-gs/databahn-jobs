package pipeline

import (
	"bytes"
	"context"
	"fmt"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/format"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"go.uber.org/zap"
)

const s3MinPartSize = 5 * 1024 * 1024

// encodeRowsToUploader streams rows through the export encoder into multipart upload parts.
func (p *Pipeline) encodeRowsToUploader(
	ctx context.Context,
	getColumns func() []string,
	streamFn func(func([]interface{}) error) error,
	onPartUploaded func(partNum int, totalRows, totalBytes int64) error,
) (int64, int64, error) {
	var totalRows int64
	var totalBytes int64
	var parts []upload.PartInfo
	partNum := 1

	exportFormat := normalizedFormat(p.request.Format)
	isExcel := exportFormat == "xlsx" || exportFormat == "excel"
	maxSegBytes := int64(p.config.MaxSegmentSizeMB) * 1024 * 1024
	if maxSegBytes <= 0 {
		maxSegBytes = 10 * 1024 * 1024
	}
	if _, ok := p.uploader.(*upload.S3Uploader); ok && maxSegBytes < s3MinPartSize {
		maxSegBytes = s3MinPartSize
	}

	var buf bytes.Buffer
	var enc format.FormatEncoder
	encoderReady := false

	newEncoder := func(columns []string) error {
		buf.Reset()
		var err error
		enc, err = format.NewEncoder(exportFormat, &buf, p.request.Delimiter)
		if err != nil {
			return err
		}
		if err := enc.Init(columns); err != nil {
			return err
		}
		return enc.WriteHeader()
	}

	ensureEncoder := func() error {
		if encoderReady {
			return nil
		}
		columns := getColumns()
		if len(columns) == 0 {
			return fmt.Errorf("export columns are required")
		}
		if err := newEncoder(columns); err != nil {
			return fmt.Errorf("failed to create encoder: %w", err)
		}
		encoderReady = true
		return nil
	}

	flushPart := func(final bool) error {
		if isExcel && !final {
			return nil
		}
		if err := enc.Finalize(); err != nil {
			return fmt.Errorf("failed to finalize encoder: %w", err)
		}
		data := buf.Bytes()
		if exportFormat == "csv" && partNum > 1 {
			if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
				data = data[idx+1:]
			}
		}
		if len(data) == 0 {
			return nil
		}
		size := int64(len(data))
		part, err := p.uploader.UploadPart(ctx, partNum, bytes.NewReader(data), size)
		if err != nil {
			return fmt.Errorf("failed to upload part %d: %w", partNum, err)
		}
		parts = append(parts, *part)
		totalBytes += size
		uploadedPart := partNum
		partNum++
		if onPartUploaded != nil {
			if err := onPartUploaded(uploadedPart, totalRows, totalBytes); err != nil {
				return err
			}
		}
		if !final {
			columns := getColumns()
			if len(columns) == 0 {
				return fmt.Errorf("export columns are required")
			}
			return newEncoder(columns)
		}
		return nil
	}

	err := streamFn(func(row []interface{}) error {
		if err := ensureEncoder(); err != nil {
			return err
		}
		if err := enc.WriteRow(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
		totalRows++
		if !isExcel && int64(buf.Len()) >= maxSegBytes {
			return flushPart(false)
		}
		return nil
	})
	if err != nil {
		return totalRows, 0, err
	}

	if !encoderReady {
		columns := getColumns()
		if len(columns) > 0 {
			if err := newEncoder(columns); err != nil {
				return totalRows, 0, fmt.Errorf("failed to create encoder for empty export: %w", err)
			}
			encoderReady = true
			if err := flushPart(true); err != nil {
				return totalRows, 0, err
			}
		}
	} else if encoderReady {
		if err := flushPart(true); err != nil {
			return totalRows, 0, err
		}
	}

	if len(parts) == 0 {
		if p.uploader != nil && p.uploader.UploadID() != "" {
			_ = p.uploader.Abort(ctx)
		}
		return 0, 0, nil
	}

	if err := p.uploader.Complete(ctx, parts); err != nil {
		return totalRows, 0, fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	p.log.Info("Export upload completed",
		zap.Int64("totalRows", totalRows),
		zap.Int64("totalBytes", totalBytes))

	return totalRows, totalBytes, nil
}
