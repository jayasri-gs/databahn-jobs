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
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/state"
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
	EFSMountPath     string // mount base for EFS checkpoint files
}

type PipelineResult struct {
	TotalRows    int64
	TotalBytes   int64
	Location     string
	PresignedURL string
	Expiry       time.Time
}

type Pipeline struct {
	config     PipelineConfig
	request    *models.SearchExportConfig
	reportID   string
	exportName string
	executor   query.QueryExecutor
	uploader   upload.CloudUploader
	log        *zap.Logger
}

func New(cfg PipelineConfig, reportID, exportName string, req *models.SearchExportConfig, executor query.QueryExecutor, log *zap.Logger) *Pipeline {
	if log == nil {
		log = logging.GetLogger()
	}
	return &Pipeline{
		config:     cfg,
		request:    req,
		reportID:   reportID,
		exportName: exportName,
		executor:   executor,
		log:        log,
	}
}

func (p *Pipeline) Run(ctx context.Context, destBucket string, onAthenaStart func(executionID string) error) (*PipelineResult, error) {
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

	executionID, err := p.executor.ExecuteUnloadAsync(ctx, p.request.Query, p.request.Database, tempS3Path, unloadOpts)
	if err != nil {
		return nil, fmt.Errorf("UNLOAD start failed: %w", err)
	}

	cp := state.Checkpoint{
		Stage:             state.StageQuerying,
		AthenaExecutionID: executionID,
		TempOutputPath:    tempS3Path,
	}
	if p.config.EFSMountPath != "" {
		if err := state.Write(p.config.EFSMountPath, p.reportID, cp); err != nil {
			return nil, fmt.Errorf("failed to write querying checkpoint: %w", err)
		}
	}

	if onAthenaStart != nil {
		if err := onAthenaStart(executionID); err != nil {
			p.log.Warn("Failed to write athena execution ID to DB", zap.Error(err))
		}
	}

	if err := p.executor.WaitForExecution(ctx, executionID); err != nil {
		if !isAthenaTimeout(err) {
			p.cleanupCheckpoint()
		}
		return nil, fmt.Errorf("UNLOAD failed: %w", err)
	}

	unloadResult, err := p.executor.GetExecutionResult(ctx, executionID)
	if err != nil {
		p.cleanupCheckpoint()
		return nil, fmt.Errorf("failed to get UNLOAD result: %w", err)
	}
	unloadResult.OutputLocation = tempS3Path

	return p.runUploadPhase(ctx, awsCfg, destBucket, unloadResult, directUpload, unload.StreamOptions{})
}

// ResumeRun picks up a stale PROCESSING job using an existing EFS checkpoint.
// If the checkpoint is missing or corrupt, it falls back to a fresh run.
func (p *Pipeline) ResumeRun(ctx context.Context, destBucket string, onAthenaStart func(executionID string) error) (*PipelineResult, error) {
	if p.config.EFSMountPath == "" {
		return p.Run(ctx, destBucket, onAthenaStart)
	}

	cp, err := state.Read(p.config.EFSMountPath, p.reportID)
	if err != nil {
		p.log.Warn("Failed to read checkpoint, falling back to fresh run", zap.Error(err))
		return p.Run(ctx, destBucket, onAthenaStart)
	}
	if cp == nil {
		p.log.Info("No checkpoint found for stale job, starting fresh")
		return p.Run(ctx, destBucket, onAthenaStart)
	}

	p.log.Info("Resuming from checkpoint",
		zap.String("stage", cp.Stage),
		zap.String("athenaExecutionID", cp.AthenaExecutionID))

	if err := p.executor.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer p.executor.Close()

	awsCfgInterface := p.executor.GetAWSConfig()
	awsCfg, ok := awsCfgInterface.(aws.Config)
	if !ok {
		return nil, fmt.Errorf("failed to get AWS config")
	}

	switch cp.Stage {
	case state.StageQuerying:
		return p.resumeFromQuerying(ctx, awsCfg, destBucket, cp, onAthenaStart)
	case state.StageUploading:
		return p.resumeFromUploading(ctx, awsCfg, destBucket, cp)
	default:
		p.log.Warn("Unknown checkpoint stage, starting fresh", zap.String("stage", cp.Stage))
		return p.Run(ctx, destBucket, onAthenaStart)
	}
}

func (p *Pipeline) resumeFromQuerying(ctx context.Context, awsCfg aws.Config, destBucket string, cp *state.Checkpoint, onAthenaStart func(string) error) (*PipelineResult, error) {
	status, err := p.executor.CheckQueryStatus(ctx, cp.AthenaExecutionID)
	if err != nil {
		p.log.Warn("Cannot check Athena status, restarting query", zap.Error(err))
		return p.Run(ctx, destBucket, onAthenaStart)
	}

	p.log.Info("Athena query status on resume", zap.String("status", status), zap.String("executionID", cp.AthenaExecutionID))

	switch status {
	case query.QueryStateRunning, query.QueryStateQueued:
		p.log.Info("Reattaching to running Athena query")
		if err := p.executor.WaitForExecution(ctx, cp.AthenaExecutionID); err != nil {
			if !isAthenaTimeout(err) {
				p.cleanupCheckpoint()
			}
			return nil, fmt.Errorf("reattached query failed: %w", err)
		}
		result, err := p.executor.GetExecutionResult(ctx, cp.AthenaExecutionID)
		if err != nil {
			p.cleanupCheckpoint()
			return nil, fmt.Errorf("get execution result after reattach: %w", err)
		}
		result.OutputLocation = cp.TempOutputPath
		unloadOpts := p.unloadOptions()
		return p.runUploadPhase(ctx, awsCfg, destBucket, result, p.useDirectUpload(unloadOpts), unload.StreamOptions{})

	case query.QueryStateSucceeded:
		p.log.Info("Athena query already succeeded, skipping to upload phase")
		result, err := p.executor.GetExecutionResult(ctx, cp.AthenaExecutionID)
		if err != nil {
			p.cleanupCheckpoint()
			return nil, fmt.Errorf("get execution result: %w", err)
		}
		result.OutputLocation = cp.TempOutputPath
		unloadOpts := p.unloadOptions()
		return p.runUploadPhase(ctx, awsCfg, destBucket, result, p.useDirectUpload(unloadOpts), unload.StreamOptions{})

	default:
		p.log.Info("Athena query not recoverable, restarting", zap.String("status", status))
		return p.Run(ctx, destBucket, onAthenaStart)
	}
}

func (p *Pipeline) resumeFromUploading(ctx context.Context, awsCfg aws.Config, destBucket string, cp *state.Checkpoint) (*PipelineResult, error) {
	unloadOpts := p.unloadOptions()
	directUpload := p.useDirectUpload(unloadOpts)

	if !directUpload {
		p.log.Info("Parquet upload resume: aborting orphaned upload, restarting from UNLOAD files")
		if cp.UploadID != "" {
			_ = upload.AbortOrphanedUpload(ctx, awsCfg, cp.Bucket, cp.Key, cp.UploadID)
		}
		fakeResult := &query.UnloadResult{
			ManifestLocation: cp.ManifestLocation,
		}
		return p.runUploadPhase(ctx, awsCfg, destBucket, fakeResult, false, unload.StreamOptions{})
	}

	p.log.Info("Direct upload resume",
		zap.String("uploadID", cp.UploadID),
		zap.Int("processedFileIndex", cp.ProcessedFileIndex),
		zap.Int("lastUploadedPart", cp.LastUploadedPart))

	s3Up := upload.NewS3UploaderFromExisting(awsCfg, cp.Bucket, cp.Key, cp.UploadID, p.config.LifecycleTag)
	existingParts, err := s3Up.ListParts(ctx)
	if err != nil || len(existingParts) == 0 {
		p.log.Warn("ListParts failed or empty — restarting upload from UNLOAD files", zap.Error(err))
		if cp.UploadID != "" {
			_ = upload.AbortOrphanedUpload(ctx, awsCfg, cp.Bucket, cp.Key, cp.UploadID)
		}
		fakeResult := &query.UnloadResult{ManifestLocation: cp.ManifestLocation}
		return p.runUploadPhase(ctx, awsCfg, destBucket, fakeResult, true, unload.StreamOptions{})
	}

	p.log.Info("Recovered existing parts",
		zap.Int("partCount", len(existingParts)),
		zap.Int("resumeFileIndex", cp.ProcessedFileIndex))

	p.uploader = s3Up
	streamOpts := unload.StreamOptions{
		StartFileIndex:  cp.ProcessedFileIndex,
		StartPartNumber: cp.LastUploadedPart + 1,
		ExistingParts:   existingParts,
	}
	if p.config.EFSMountPath != "" {
		s3upRef := s3Up
		streamOpts.OnFileCheckpoint = func(fileIndex, partNum int, rows, bytes int64) {
			newCp := state.Checkpoint{
				Stage:              state.StageUploading,
				UploadID:           s3upRef.UploadID(),
				Bucket:             cp.Bucket,
				Key:                cp.Key,
				UnloadFiles:        cp.UnloadFiles,
				ManifestLocation:   cp.ManifestLocation,
				ProcessedFileIndex: fileIndex,
				LastUploadedPart:   partNum,
				RowsProcessed:      rows,
				BytesProcessed:     bytes,
			}
			if werr := state.Write(p.config.EFSMountPath, p.reportID, newCp); werr != nil {
				p.log.Warn("Failed to write file checkpoint on resume", zap.Error(werr))
			}
		}
	}

	unloadReader, err := unload.NewReader(awsCfg, p.config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init unload reader: %w", err)
	}

	totalRows, totalBytes, err := p.processUnloadDirect(ctx, unloadReader, cp.UnloadFiles, streamOpts)
	if err != nil {
		p.uploader.Abort(ctx)
		unloadReader.DeleteS3Files(ctx, cp.UnloadFiles)
		p.cleanupCheckpoint()
		return nil, fmt.Errorf("resume upload failed: %w", err)
	}

	unloadReader.DeleteS3Files(ctx, cp.UnloadFiles)
	if cp.ManifestLocation != "" {
		unloadReader.DeleteS3Files(ctx, []string{cp.ManifestLocation})
	}

	presignedURL, err := p.uploader.GeneratePresignedURL(ctx, p.config.PresignExpiry)
	if err != nil {
		p.log.Warn("Failed to generate presigned URL", zap.Error(err))
	}

	p.cleanupCheckpoint()

	return &PipelineResult{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		Location:     p.uploader.GetLocation(),
		PresignedURL: presignedURL,
		Expiry:       time.Now().Add(p.config.PresignExpiry),
	}, nil
}

func (p *Pipeline) runUploadPhase(
	ctx context.Context,
	awsCfg aws.Config,
	destBucket string,
	unloadResult *query.UnloadResult,
	directUpload bool,
	streamOpts unload.StreamOptions,
) (*PipelineResult, error) {
	unloadReader, err := unload.NewReader(awsCfg, p.config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init unload reader: %w", err)
	}

	var outputFiles []string
	if unloadResult.ManifestLocation != "" {
		outputFiles, err = unloadReader.ParseManifest(ctx, unloadResult.ManifestLocation)
	} else {
		outputFiles, err = unloadReader.ListFiles(ctx, unloadResult.OutputLocation)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list UNLOAD output: %w", err)
	}

	if len(outputFiles) == 0 {
		p.log.Warn("UNLOAD produced no files")
		p.cleanupCheckpoint()
		return &PipelineResult{TotalRows: 0}, nil
	}

	exportFormat := normalizedFormat(p.request.Format)
	ext, contentType := formatMeta(exportFormat)
	fileName := exportFileName(p.exportName, p.request.TableName, p.request.DataSetName, ext, p.reportID)
	outputKey := fmt.Sprintf("exports/%s/%s", p.reportID, fileName)

	p.log.Info("Export output file", zap.String("fileName", fileName), zap.String("outputKey", outputKey))

	p.uploader = upload.NewS3Uploader(awsCfg, p.config.LifecycleTag)
	if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
		return nil, fmt.Errorf("failed to init uploader: %w", err)
	}

	s3Up := p.uploader.(*upload.S3Uploader)

	if p.config.EFSMountPath != "" {
		cp := state.Checkpoint{
			Stage:            state.StageUploading,
			UploadID:         s3Up.UploadID(),
			Bucket:           destBucket,
			Key:              outputKey,
			UnloadFiles:      outputFiles,
			ManifestLocation: unloadResult.ManifestLocation,
		}
		if err := state.Write(p.config.EFSMountPath, p.reportID, cp); err != nil {
			p.uploader.Abort(ctx)
			return nil, fmt.Errorf("failed to write uploading checkpoint: %w", err)
		}
	}

	if directUpload && p.config.EFSMountPath != "" {
		streamOpts.OnFileCheckpoint = func(fileIndex, partNum int, rows, bytes int64) {
			cp := state.Checkpoint{
				Stage:              state.StageUploading,
				UploadID:           s3Up.UploadID(),
				Bucket:             destBucket,
				Key:                outputKey,
				UnloadFiles:        outputFiles,
				ManifestLocation:   unloadResult.ManifestLocation,
				ProcessedFileIndex: fileIndex,
				LastUploadedPart:   partNum,
				RowsProcessed:      rows,
				BytesProcessed:     bytes,
			}
			if err := state.Write(p.config.EFSMountPath, p.reportID, cp); err != nil {
				p.log.Warn("Failed to write file checkpoint", zap.Error(err))
			}
		}
	}

	var totalRows, totalBytes int64
	if directUpload {
		totalRows, totalBytes, err = p.processUnloadDirect(ctx, unloadReader, outputFiles, streamOpts)
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
		p.cleanupCheckpoint()
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

	p.cleanupCheckpoint()

	return &PipelineResult{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		Location:     p.uploader.GetLocation(),
		PresignedURL: presignedURL,
		Expiry:       time.Now().Add(p.config.PresignExpiry),
	}, nil
}

func (p *Pipeline) cleanupCheckpoint() {
	if p.config.EFSMountPath != "" {
		_ = state.Delete(p.config.EFSMountPath, p.reportID)
	}
}

func isAthenaTimeout(err error) bool {
	return err != nil && strings.Contains(err.Error(), "timed out")
}

func formatMeta(exportFormat string) (ext, contentType string) {
	switch exportFormat {
	case "json":
		return "json", "application/x-ndjson"
	case "xlsx", "excel":
		return "xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	default:
		return "csv", "text/csv"
	}
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

func (p *Pipeline) processUnloadDirect(ctx context.Context, ur *unload.Reader, files []string, opts unload.StreamOptions) (int64, int64, error) {
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
	return ur.StreamToUploader(ctx, p.uploader, files, header, normalizedFormat(p.request.Format), delim, p.log, opts)
}

func trimSuffix(s, suffix string) string {
	if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
		return s[:len(s)-len(suffix)]
	}
	return s
}

// exportFileName builds SearchExport_<table>_<datetime>.<ext>.
func exportFileName(displayName, tableName, dataSetName, ext, reportID string) string {
	table := sanitizeFileBase(strings.TrimSpace(tableName))
	if table == "" {
		table = sanitizeFileBase(strings.TrimSpace(dataSetName))
	}
	if table == "" {
		table = "data"
		if len(reportID) >= 8 {
			table = reportID[:8]
		}
	}
	base := fmt.Sprintf("SearchExport_%s_%s", table, exportDateTime(displayName))
	if len(base) > 120 {
		base = strings.Trim(base[:120], "._")
	}
	return base + "." + ext
}

// exportDateTime parses the timestamp from the audit report name, e.g. "Search Export - 2026-06-11 18:50:13".
func exportDateTime(displayName string) string {
	const sep = " - "
	if idx := strings.LastIndex(displayName, sep); idx >= 0 {
		raw := strings.TrimSpace(displayName[idx+len(sep):])
		if t, err := time.Parse("2006-01-02 15:04:05", raw); err == nil {
			return t.Format("2006-01-02_15-04-05")
		}
	}
	return time.Now().UTC().Format("2006-01-02_15-04-05")
}

func sanitizeFileBase(name string) string {
	if name == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(name))
	prevUnderscore := false
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevUnderscore = false
		case r == '-' || r == '.':
			b.WriteRune(r)
			prevUnderscore = false
		default:
			if !prevUnderscore {
				b.WriteByte('_')
				prevUnderscore = true
			}
		}
	}
	out := strings.Trim(b.String(), "._")
	if len(out) > 120 {
		out = strings.Trim(out[:120], "._")
	}
	return out
}
