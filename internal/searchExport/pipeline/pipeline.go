package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	athena     query.UnloadExecutor
	synapse    query.RowStreamExecutor
	uploader   upload.CloudUploader
	log        *zap.Logger
}

func New(cfg PipelineConfig, reportID, exportName string, req *models.SearchExportConfig, athena query.UnloadExecutor, synapse query.RowStreamExecutor, uploader upload.CloudUploader, log *zap.Logger) *Pipeline {
	if log == nil {
		log = logging.GetLogger()
	}
	return &Pipeline{
		config:     cfg,
		request:    req,
		reportID:   reportID,
		exportName: exportName,
		athena:     athena,
		synapse:    synapse,
		uploader:   uploader,
		log:        log,
	}
}

func (p *Pipeline) buildTempOutputPath() string {
	tempPath := fmt.Sprintf("unload_%s_%d/", p.reportID, time.Now().Unix())
	athenaOutputLoc := p.athena.GetOutputLocation()
	return trimSuffix(athenaOutputLoc, "/") + "/" + tempPath
}

// stagingListPrefix prefers the executor result (e.g. Synapse CETAS blob prefix);
// Athena UNLOAD often leaves OutputLocation empty and relies on tempOutputPath instead.
func stagingListPrefix(executorLocation, fallback string) string {
	if executorLocation != "" {
		return executorLocation
	}
	return normalizeBlobStagingPrefix(fallback)
}

func normalizeBlobStagingPrefix(path string) string {
	path = strings.TrimPrefix(path, "/")
	if after, ok := strings.CutPrefix(path, ".databahn_out/"); ok {
		return strings.Trim(after, "/")
	}
	return path
}

func checkpointExecutionID(cp *state.Checkpoint) string {
	if cp.QueryExecutionID != "" {
		return cp.QueryExecutionID
	}
	return cp.AthenaExecutionID
}

func (p *Pipeline) Run(ctx context.Context, destBucket string, onAthenaStart func(executionID string) error) (*PipelineResult, error) {
	p.log.Info("Starting export pipeline", zap.String("destBucket", destBucket))

	if p.synapse != nil {
		if err := p.synapse.Connect(ctx); err != nil {
			return nil, fmt.Errorf("failed to connect: %w", err)
		}
		defer p.synapse.Close()
		return p.runSynapseStreamExport(ctx, destBucket, p.synapse)
	}

	if p.athena == nil {
		return nil, fmt.Errorf("no export executor configured")
	}

	if err := p.athena.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer p.athena.Close()

	tempOutputPath := p.buildTempOutputPath()

	unloadOpts := p.unloadOptions()
	directUpload := p.useDirectUpload(unloadOpts)
	p.log.Info("Executing server-side export",
		zap.String("tempOutputPath", tempOutputPath),
		zap.String("engine", p.athena.Engine()),
		zap.String("unloadFormat", unloadOpts.Format),
		zap.Bool("directUpload", directUpload))

	executionID, err := p.athena.ExecuteUnloadAsync(ctx, p.request.Query, p.request.Database, tempOutputPath, unloadOpts)
	if err != nil {
		return nil, fmt.Errorf("UNLOAD start failed: %w", err)
	}

	cp := state.Checkpoint{
		Stage:             state.StageQuerying,
		QueryExecutionID:  executionID,
		AthenaExecutionID: executionID,
		TempOutputPath:    tempOutputPath,
	}
	if p.config.EFSMountPath != "" {
		if err := state.Write(p.config.EFSMountPath, p.reportID, cp); err != nil {
			if cancelErr := p.athena.CancelQueryExecution(ctx, executionID); cancelErr != nil {
				p.log.Warn("Failed to cancel Athena query after checkpoint write failure", zap.Error(cancelErr))
			}
			return nil, fmt.Errorf("failed to write querying checkpoint: %w", err)
		}
	}

	if onAthenaStart != nil {
		if err := onAthenaStart(executionID); err != nil {
			p.log.Warn("Failed to write athena execution ID to DB", zap.Error(err))
		}
	}

	if err := p.athena.WaitForExecution(ctx, executionID); err != nil {
		if !shouldPreserveQueryCheckpoint(err) {
			p.cleanupCheckpoint()
		}
		return nil, fmt.Errorf("UNLOAD failed: %w", err)
	}

	unloadResult, err := p.athena.GetExecutionResult(ctx, executionID)
	if err != nil {
		p.cleanupCheckpoint()
		return nil, fmt.Errorf("failed to get UNLOAD result: %w", err)
	}
	unloadResult.OutputLocation = stagingListPrefix(unloadResult.OutputLocation, tempOutputPath)

	return p.runUploadPhase(ctx, destBucket, unloadResult, directUpload, unload.StreamOptions{}, nil)
}

// ResumeRun picks up a stale PROCESSING job using an existing EFS checkpoint.
// If the checkpoint is missing or corrupt, it falls back to a fresh run.
func (p *Pipeline) ResumeRun(ctx context.Context, destBucket string, onAthenaStart func(executionID string) error) (*PipelineResult, error) {
	if p.synapse != nil {
		return p.runSynapseStreamExport(ctx, destBucket, p.synapse)
	}

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

	if p.athena == nil {
		return nil, fmt.Errorf("no athena executor configured")
	}

	if err := p.athena.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer p.athena.Close()

	awsCfg, awsErr := p.executorAWSConfig()

	switch cp.Stage {
	case state.StageQuerying:
		return p.resumeFromQuerying(ctx, awsCfg, awsErr, destBucket, cp, onAthenaStart)
	case state.StageUploading:
		return p.resumeFromUploading(ctx, awsCfg, awsErr, destBucket, cp)
	default:
		p.log.Warn("Unknown checkpoint stage, starting fresh", zap.String("stage", cp.Stage))
		return p.Run(ctx, destBucket, onAthenaStart)
	}
}

func (p *Pipeline) executorAWSConfig() (aws.Config, error) {
	if p.athena == nil {
		return aws.Config{}, fmt.Errorf("athena executor not configured")
	}
	if cfg, ok := p.athena.GetAWSConfig().(aws.Config); ok {
		return cfg, nil
	}
	return aws.Config{}, fmt.Errorf("executor does not provide AWS config")
}

func (p *Pipeline) resumeFromQuerying(ctx context.Context, awsCfg aws.Config, awsErr error, destBucket string, cp *state.Checkpoint, onAthenaStart func(string) error) (*PipelineResult, error) {
	execID := checkpointExecutionID(cp)
	status, err := p.athena.CheckQueryStatus(ctx, execID)
	if err != nil {
		p.log.Warn("Cannot check Athena status, restarting query", zap.Error(err))
		return p.Run(ctx, destBucket, onAthenaStart)
	}

	p.log.Info("Query status on resume", zap.String("status", status), zap.String("executionID", execID))

	switch status {
	case query.QueryStateRunning, query.QueryStateQueued:
		p.log.Info("Reattaching to running query")
		if err := p.athena.WaitForExecution(ctx, execID); err != nil {
			if !shouldPreserveQueryCheckpoint(err) {
				p.cleanupCheckpoint()
			}
			return nil, fmt.Errorf("reattached query failed: %w", err)
		}
		result, err := p.athena.GetExecutionResult(ctx, execID)
		if err != nil {
			p.cleanupCheckpoint()
			return nil, fmt.Errorf("get execution result after reattach: %w", err)
		}
		result.OutputLocation = stagingListPrefix(result.OutputLocation, cp.TempOutputPath)
		unloadOpts := p.unloadOptions()
		return p.runUploadPhase(ctx, destBucket, result, p.useDirectUpload(unloadOpts), unload.StreamOptions{}, nil)

	case query.QueryStateSucceeded:
		p.log.Info("Query already succeeded, skipping to upload phase")
		result, err := p.athena.GetExecutionResult(ctx, execID)
		if err != nil {
			p.cleanupCheckpoint()
			return nil, fmt.Errorf("get execution result: %w", err)
		}
		result.OutputLocation = stagingListPrefix(result.OutputLocation, cp.TempOutputPath)
		unloadOpts := p.unloadOptions()
		return p.runUploadPhase(ctx, destBucket, result, p.useDirectUpload(unloadOpts), unload.StreamOptions{}, nil)

	default:
		p.log.Info("Query not recoverable, restarting", zap.String("status", status))
		return p.Run(ctx, destBucket, onAthenaStart)
	}
}

func (p *Pipeline) resumeFromUploading(ctx context.Context, awsCfg aws.Config, awsErr error, destBucket string, cp *state.Checkpoint) (*PipelineResult, error) {
	unloadOpts := p.unloadOptions()
	directUpload := p.useDirectUpload(unloadOpts)

	if !directUpload || awsErr != nil {
		p.log.Info("Upload resume: restarting from staging files")
		return p.restartUploadFromCheckpoint(ctx, awsCfg, awsErr, destBucket, cp, directUpload)
	}

	if _, isS3Uploader := p.uploader.(*upload.S3Uploader); !isS3Uploader {
		p.log.Info("Non-S3 export destination resume: restarting from staging files")
		return p.restartUploadFromCheckpoint(ctx, awsCfg, awsErr, destBucket, cp, true)
	}

	p.log.Info("Direct upload resume",
		zap.String("uploadID", cp.UploadID),
		zap.Int("processedFileIndex", cp.ProcessedFileIndex),
		zap.Int("lastUploadedPart", cp.LastUploadedPart))

	s3Up := upload.NewS3UploaderFromExisting(awsCfg, cp.Bucket, cp.Key, cp.UploadID, p.config.LifecycleTag)
	existingParts, err := s3Up.ListParts(ctx)
	if err != nil || len(existingParts) == 0 {
		p.log.Warn("ListParts failed or empty — restarting upload from UNLOAD files", zap.Error(err))
		return p.restartUploadFromCheckpoint(ctx, awsCfg, awsErr, destBucket, cp, true)
	}

	p.log.Info("Recovered existing parts",
		zap.Int("partCount", len(existingParts)),
		zap.Int("resumeFileIndex", cp.ProcessedFileIndex))

	// Only include parts at or before the last checkpointed boundary. Parts uploaded
	// after the checkpoint but before a crash are orphaned on S3 and must not be
	// passed to Complete — they will be overwritten when we resume at LastUploadedPart+1.
	committedParts := make([]upload.PartInfo, 0, cp.LastUploadedPart)
	for _, part := range existingParts {
		if part.PartNumber <= cp.LastUploadedPart {
			committedParts = append(committedParts, part)
		}
	}
	if len(committedParts) < len(existingParts) {
		p.log.Warn("Ignoring S3 parts beyond checkpoint boundary",
			zap.Int("checkpointedPart", cp.LastUploadedPart),
			zap.Int("listedParts", len(existingParts)),
			zap.Int("committedParts", len(committedParts)))
	}

	if cp.LastUploadedPart > 0 && len(committedParts) != cp.LastUploadedPart {
		p.log.Warn("S3 committed parts do not match checkpoint — restarting upload from UNLOAD files",
			zap.Int("checkpointedPart", cp.LastUploadedPart),
			zap.Int("committedParts", len(committedParts)))
		return p.restartUploadFromCheckpoint(ctx, awsCfg, awsErr, destBucket, cp, true)
	}

	p.uploader = s3Up
	streamOpts := unload.StreamOptions{
		StartFileIndex:  cp.ProcessedFileIndex,
		StartPartNumber: cp.LastUploadedPart + 1,
		ExistingParts:   committedParts,
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
				TempOutputPath:     cp.TempOutputPath,
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

	unloadReader, err := p.athena.NewStagingReader(p.config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init staging reader: %w", err)
	}

	totalRows, totalBytes, err := p.processUnloadDirect(ctx, unloadReader, cp.UnloadFiles, streamOpts)
	if err != nil {
		p.uploader.Abort(ctx)
		_ = unloadReader.DeleteFiles(ctx, cp.UnloadFiles)
		p.cleanupCheckpoint()
		return nil, fmt.Errorf("resume upload failed: %w", err)
	}

	_ = unloadReader.DeleteFiles(ctx, cp.UnloadFiles)
	if cp.ManifestLocation != "" {
		_ = unloadReader.DeleteFiles(ctx, []string{cp.ManifestLocation})
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
	destBucket string,
	unloadResult *query.UnloadResult,
	directUpload bool,
	streamOpts unload.StreamOptions,
	preloadFiles []string,
) (*PipelineResult, error) {
	if p.uploader == nil {
		return nil, fmt.Errorf("uploader not configured")
	}

	unloadReader, err := p.athena.NewStagingReader(p.config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init staging reader: %w", err)
	}

	var outputFiles []string
	if len(preloadFiles) > 0 {
		outputFiles = preloadFiles
	} else if unloadResult.ManifestLocation != "" {
		outputFiles, err = unloadReader.ParseManifest(ctx, unloadResult.ManifestLocation)
	} else if unloadResult.OutputLocation != "" {
		outputFiles, err = unloadReader.ListFiles(ctx, unloadResult.OutputLocation)
	} else {
		return nil, fmt.Errorf("no UNLOAD manifest, output location, or preloaded files")
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

	if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
		return nil, fmt.Errorf("failed to init uploader: %w", err)
	}

	tempOutputPath := unloadResult.OutputLocation

	if p.config.EFSMountPath != "" {
		cp := state.Checkpoint{
			Stage:            state.StageUploading,
			UploadID:         p.uploader.UploadID(),
			Bucket:           destBucket,
			Key:              outputKey,
			UnloadFiles:      outputFiles,
			ManifestLocation: unloadResult.ManifestLocation,
			TempOutputPath:   tempOutputPath,
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
				UploadID:           p.uploader.UploadID(),
				Bucket:             destBucket,
				Key:                outputKey,
				UnloadFiles:        outputFiles,
				ManifestLocation:   unloadResult.ManifestLocation,
				TempOutputPath:     tempOutputPath,
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
		_ = unloadReader.DeleteFiles(ctx, outputFiles)
		p.cleanupCheckpoint()
		return nil, err
	}

	_ = unloadReader.DeleteFiles(ctx, outputFiles)
	if unloadResult.ManifestLocation != "" {
		_ = unloadReader.DeleteFiles(ctx, []string{unloadResult.ManifestLocation})
	}

	if totalBytes == 0 {
		p.cleanupCheckpoint()
		return &PipelineResult{TotalRows: totalRows, TotalBytes: 0}, nil
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

func (p *Pipeline) abortCheckpointedUpload(ctx context.Context, cp *state.Checkpoint) {
	if cp == nil || cp.UploadID == "" {
		return
	}
	if s3up, ok := p.uploader.(*upload.S3Uploader); ok {
		if err := s3up.AbortOrphaned(ctx, cp.Bucket, cp.Key, cp.UploadID); err != nil {
			p.log.Warn("Failed to abort orphaned S3 multipart upload", zap.Error(err))
		}
		return
	}
	if az, ok := p.uploader.(*upload.AzureUploader); ok {
		if err := az.AbortInFlight(ctx, cp.Bucket, cp.Key); err != nil {
			p.log.Warn("Failed to abort orphaned Azure blob upload", zap.Error(err))
		}
		return
	}
	if p.uploader != nil {
		_ = p.uploader.Abort(ctx)
	}
}

func unloadResultFromCheckpoint(cp *state.Checkpoint) *query.UnloadResult {
	return &query.UnloadResult{
		ManifestLocation: cp.ManifestLocation,
		OutputLocation:   normalizeBlobStagingPrefix(cp.TempOutputPath),
	}
}

func (p *Pipeline) restartUploadFromCheckpoint(
	ctx context.Context,
	awsCfg aws.Config,
	awsErr error,
	destBucket string,
	cp *state.Checkpoint,
	directUpload bool,
) (*PipelineResult, error) {
	p.abortCheckpointedUpload(ctx, cp)
	return p.runUploadPhase(ctx, destBucket, unloadResultFromCheckpoint(cp), directUpload, unload.StreamOptions{}, cp.UnloadFiles)
}

func shouldPreserveQueryCheckpoint(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return strings.Contains(err.Error(), "timed out")
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

func (p *Pipeline) processUnloadToFinal(ctx context.Context, ur unload.StagingReader, files []string) (int64, int64, error) {
	totalRows, totalBytes, err := p.encodeRowsToUploader(ctx, func() []string { return ur.Columns() }, func(cb func([]interface{}) error) error {
		return ur.StreamRows(ctx, files, cb)
	}, nil, nil)
	if err != nil {
		return totalRows, 0, fmt.Errorf("failed to process Parquet: %w", err)
	}
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

func (p *Pipeline) processUnloadDirect(ctx context.Context, ur unload.StagingReader, files []string, opts unload.StreamOptions) (int64, int64, error) {
	var header []byte
	if p.request.IncludeHeader && normalizedFormat(p.request.Format) == "csv" {
		columns, err := p.athena.GetQueryColumns(ctx, p.request.Query, p.request.Database)
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
