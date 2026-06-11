package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/format"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type PipelineConfig struct {
	TempDir          string // used by unload reader to cache parquet files locally
	MaxSegmentSizeMB int
	PresignExpiry    time.Duration
	LifecycleTag     string
}

type PipelineResult struct {
	TotalRows    int64
	TotalBytes   int64
	Location     string
	PresignedURL string
	Expiry       time.Time
}

type Pipeline struct {
	config   PipelineConfig
	request  *models.SearchExportConfig
	reportID string
	executor query.QueryExecutor
	uploader upload.CloudUploader
	log      *zap.Logger
}

func New(cfg PipelineConfig, reportID string, req *models.SearchExportConfig, executor query.QueryExecutor, log *zap.Logger) *Pipeline {
	if log == nil {
		log = logging.GetLogger()
	}
	return &Pipeline{
		config:   cfg,
		request:  req,
		reportID: reportID,
		executor: executor,
		log:      log,
	}
}

func (p *Pipeline) Run(ctx context.Context, destBucket string) (*PipelineResult, error) {
	p.log.Info("Starting export pipeline", zap.String("destBucket", destBucket))

	if err := p.executor.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer p.executor.Close()

	awsCfgInterface := p.executor.GetAWSConfig()
	awsCfg, ok := awsCfgInterface.(aws.Config)
	if !ok {
		return nil, fmt.Errorf("failed to get AWS config")
	}

	athenaOutputLoc := p.executor.GetOutputLocation()
	if athenaOutputLoc == "" {
		return nil, fmt.Errorf("athena output_location required for UNLOAD")
	}

	tempPath := fmt.Sprintf("unload_%s_%d/", p.reportID, time.Now().Unix())
	tempS3Path := trimSuffix(athenaOutputLoc, "/") + "/" + tempPath

	unloadOpts := p.unloadOptions()
	directUpload := p.useDirectUpload(unloadOpts)
	p.log.Info("Executing UNLOAD",
		zap.String("tempS3Path", tempS3Path),
		zap.String("unloadFormat", unloadOpts.Format),
		zap.Bool("directUpload", directUpload))

	unloadResult, err := p.executor.ExecuteUnload(ctx, p.request.Query, p.request.Database, tempS3Path, unloadOpts)
	if err != nil {
		return nil, fmt.Errorf("UNLOAD failed: %w", err)
	}

	unloadReader, err := unload.NewReader(awsCfg, p.config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init unload reader: %w", err)
	}

	var outputFiles []string
	if unloadResult.ManifestLocation != "" {
		outputFiles, err = unloadReader.ParseManifest(ctx, unloadResult.ManifestLocation)
	} else {
		outputFiles, err = unloadReader.ListFiles(ctx, tempS3Path)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list UNLOAD output: %w", err)
	}

	if len(outputFiles) == 0 {
		p.log.Warn("UNLOAD produced no files")
		return &PipelineResult{TotalRows: 0}, nil
	}

	exportFormat := normalizedFormat(p.request.Format)
	ext := "csv"
	contentType := "text/csv"
	switch exportFormat {
	case "json":
		ext = "json"
		contentType = "application/x-ndjson"
	case "xlsx", "excel":
		ext = "xlsx"
		contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	}

	outputKey := fmt.Sprintf("exports/%s/export.%s", p.reportID, ext)

	p.uploader = upload.NewS3Uploader(awsCfg, p.config.LifecycleTag)
	if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
		return nil, fmt.Errorf("failed to init uploader: %w", err)
	}

	var totalRows, totalBytes int64
	if directUpload {
		totalRows, totalBytes, err = p.processUnloadDirect(ctx, unloadReader, outputFiles)
	} else {
		p.log.Warn("Using slow Parquet conversion path; CSV/JSON exports should use direct S3 streaming",
			zap.String("format", p.request.Format),
			zap.String("normalizedFormat", exportFormat),
			zap.Int("fileCount", len(outputFiles)))
		totalRows, totalBytes, err = p.processUnloadToFinal(ctx, unloadReader, outputFiles)
	}
	if err != nil {
		p.uploader.Abort(ctx)
		unloadReader.DeleteS3Files(ctx, outputFiles)
		return nil, err
	}

	unloadReader.DeleteS3Files(ctx, outputFiles)
	if unloadResult.ManifestLocation != "" {
		unloadReader.DeleteS3Files(ctx, []string{unloadResult.ManifestLocation})
	}

	presignedURL, err := p.uploader.GeneratePresignedURL(ctx, p.config.PresignExpiry)
	if err != nil {
		p.log.Warn("Failed to generate presigned URL", zap.Error(err))
	}

	return &PipelineResult{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		Location:     p.uploader.GetLocation(),
		PresignedURL: presignedURL,
		Expiry:       time.Now().Add(p.config.PresignExpiry),
	}, nil
}

func (p *Pipeline) processUnloadToFinal(ctx context.Context, ur *unload.Reader, files []string) (int64, int64, error) {
	var totalRows int64
	var totalBytes int64
	var parts []upload.PartInfo
	partNum := 1

	exportFormat := normalizedFormat(p.request.Format)
	isExcel := exportFormat == "xlsx" || exportFormat == "excel"
	maxSegBytes := int64(p.config.MaxSegmentSizeMB) * 1024 * 1024
	if maxSegBytes == 0 {
		maxSegBytes = 10 * 1024 * 1024
	}

	var buf bytes.Buffer
	var enc format.FormatEncoder
	var columns []string

	newEncoder := func() error {
		buf.Reset()
		var err error
		enc, err = format.NewEncoder(exportFormat, &buf, p.request.Delimiter)
		if err != nil {
			return err
		}
		if columns != nil {
			if err := enc.Init(columns); err != nil {
				return err
			}
			return enc.WriteHeader()
		}
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
		partNum++
		if !final {
			return newEncoder()
		}
		return nil
	}

	if err := newEncoder(); err != nil {
		return 0, 0, fmt.Errorf("failed to create encoder: %w", err)
	}

	err := ur.StreamRows(ctx, files, func(row []interface{}) error {
		if columns == nil {
			columns = ur.Columns()
			if err := enc.Init(columns); err != nil {
				return fmt.Errorf("failed to init encoder: %w", err)
			}
			if err := enc.WriteHeader(); err != nil {
				return fmt.Errorf("failed to write header: %w", err)
			}
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
		return totalRows, 0, fmt.Errorf("failed to process Parquet: %w", err)
	}

	if err := flushPart(true); err != nil {
		return totalRows, 0, err
	}

	if len(parts) == 0 {
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

func normalizedFormat(format string) string {
	f := strings.ToLower(strings.TrimSpace(format))
	if f == "" {
		return "csv"
	}
	return f
}

func (p *Pipeline) unloadOptions() query.UnloadOptions {
	exportFormat := normalizedFormat(p.request.Format)
	switch exportFormat {
	case "json":
		return query.UnloadOptions{Format: "json"}
	case "csv":
		delim := p.request.Delimiter
		if delim == "" {
			delim = ","
		}
		if utf8.RuneCountInString(delim) == 1 {
			return query.UnloadOptions{Format: "textfile", Delimiter: delim}
		}
		p.log.Warn("CSV export falling back to Parquet UNLOAD due to multi-character delimiter",
			zap.String("delimiter", delim))
	case "xlsx", "excel":
		// Excel requires local Parquet conversion.
	default:
		p.log.Warn("Unknown export format, using Parquet UNLOAD",
			zap.String("format", p.request.Format))
	}
	return query.UnloadOptions{Format: "parquet"}
}

func (p *Pipeline) useDirectUpload(opts query.UnloadOptions) bool {
	return opts.Format == "textfile" || opts.Format == "json"
}

func (p *Pipeline) processUnloadDirect(ctx context.Context, ur *unload.Reader, files []string) (int64, int64, error) {
	var header []byte
	if p.request.IncludeHeader && normalizedFormat(p.request.Format) == "csv" {
		columns, err := p.executor.GetQueryColumns(ctx, p.request.Query, p.request.Database)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to get column names for header: %w", err)
		}
		if len(columns) > 0 {
			delim := p.request.Delimiter
			if delim == "" {
				delim = ","
			}
			header = unload.BuildCSVHeader(columns, delim)
		}
	}

	p.log.Info("Streaming UNLOAD output directly to export destination",
		zap.Int("fileCount", len(files)),
		zap.Bool("includeHeader", len(header) > 0))

	delim := p.request.Delimiter
	if delim == "" {
		delim = ","
	}
	return ur.StreamToUploader(ctx, p.uploader, files, header, normalizedFormat(p.request.Format), delim, p.log)
}

func trimSuffix(s, suffix string) string {
	if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
		return s[:len(s)-len(suffix)]
	}
	return s
}
