package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
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
	config         PipelineConfig
	request        *models.SearchExportConfig
	reportID       string
	exportName     string
	unloadExec     query.UnloadExecutor
	rowStream      query.RowStreamExecutor
	cetasExec      query.CETASExecutor
	stagingBlobCfg *destination.AzureBlobConfig
	uploader       upload.CloudUploader
	log            *zap.Logger
}

// Deps are the collaborators a run needs. Exactly one of Unload and RowStream is set,
// depending on the engine the report selected.
type Deps struct {
	Unload      query.UnloadExecutor
	RowStream   query.RowStreamExecutor
	Uploader    upload.CloudUploader
	StagingBlob *destination.AzureBlobConfig
}

func New(cfg PipelineConfig, reportID, exportName string, req *models.SearchExportConfig, deps Deps, log *zap.Logger) *Pipeline {
	if log == nil {
		log = logging.GetLogger()
	}
	p := &Pipeline{
		config:         cfg,
		request:        req,
		reportID:       reportID,
		exportName:     exportName,
		unloadExec:     deps.Unload,
		rowStream:      deps.RowStream,
		uploader:       deps.Uploader,
		stagingBlobCfg: deps.StagingBlob,
		log:            log,
	}
	if deps.StagingBlob != nil {
		if ce, ok := deps.RowStream.(query.CETASExecutor); ok {
			p.cetasExec = ce
		}
	}
	return p
}

func (p *Pipeline) buildTempOutputPath() string {
	tempPath := fmt.Sprintf("unload_%s_%d/", p.reportID, time.Now().Unix())
	athenaOutputLoc := p.unloadExec.GetOutputLocation()
	return trimSuffix(athenaOutputLoc, "/") + "/" + tempPath
}

// stagingListPrefix prefers the executor result over the generated tempOutputPath fallback.
func stagingListPrefix(executorLocation, fallback string) string {
	if executorLocation != "" {
		return executorLocation
	}
	return normalizeBlobStagingPrefix(fallback)
}

func normalizeBlobStagingPrefix(path string) string {
	path = strings.TrimPrefix(path, "/")
	if after, ok := strings.CutPrefix(path, "databahn_out/"); ok {
		return strings.Trim(after, "/")
	}
	return path
}

func (p *Pipeline) Run(ctx context.Context, destBucket string, onQueryStart func(executionID string) error) (*PipelineResult, error) {
	p.log.Info("Starting export pipeline", zap.String("destBucket", destBucket))

	if p.rowStream != nil {
		if err := p.rowStream.Connect(ctx); err != nil {
			return nil, fmt.Errorf("failed to connect: %w", err)
		}
		defer p.rowStream.Close()
		switch p.rowStream.Engine() {
		case query.EngineSentinelLAW, query.EngineSentinelLake:
			return p.runSentinelStreamExport(ctx, destBucket, p.rowStream)
		}
		if p.cetasExec != nil && p.stagingBlobCfg != nil {
			return p.runSynapseCETASExport(ctx, destBucket, p.cetasExec, p.stagingBlobCfg)
		}
		return p.runSynapseStreamExport(ctx, destBucket, p.rowStream)
	}

	if p.unloadExec == nil {
		return nil, fmt.Errorf("no export executor configured")
	}

	if err := p.unloadExec.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer p.unloadExec.Close()

	tempOutputPath := p.buildTempOutputPath()

	unloadOpts := p.unloadOptions()
	directUpload := p.useDirectUpload(unloadOpts)
	p.log.Info("Executing server-side export",
		zap.String("tempOutputPath", tempOutputPath),
		zap.String("engine", p.unloadExec.Engine()),
		zap.String("unloadFormat", unloadOpts.Format),
		zap.Bool("directUpload", directUpload))

	executionID, err := p.unloadExec.ExecuteUnloadAsync(ctx, p.request.Query, p.request.Database, tempOutputPath, unloadOpts)
	if err != nil {
		return nil, fmt.Errorf("UNLOAD start failed: %w", err)
	}

	if onQueryStart != nil {
		if err := onQueryStart(executionID); err != nil {
			p.log.Warn("Failed to write athena execution ID to DB", zap.Error(err))
		}
	}

	if err := p.unloadExec.WaitForExecution(ctx, executionID); err != nil {
		return nil, fmt.Errorf("UNLOAD failed: %w", err)
	}

	unloadResult, err := p.unloadExec.GetExecutionResult(ctx, executionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get UNLOAD result: %w", err)
	}
	unloadResult.OutputLocation = stagingListPrefix(unloadResult.OutputLocation, tempOutputPath)

	return p.runUploadPhase(ctx, destBucket, unloadResult, directUpload)
}

// ResumeRun picks up a stale PROCESSING job. Row-stream exports always restart from
// scratch: neither Synapse nor Sentinel has an execution ID or status API — a Synapse
// query runs synchronously over JDBC and dies with the connection, and Log Analytics runs
// the query inside the HTTP request. Athena and ADX exports reattach to the execution
// recorded in the DB when it is still running or already succeeded.
// The upload to the final destination always restarts from the beginning.
func (p *Pipeline) ResumeRun(ctx context.Context, destBucket, queryExecutionID string, onQueryStart func(executionID string) error) (*PipelineResult, error) {
	if p.rowStream != nil || queryExecutionID == "" {
		return p.Run(ctx, destBucket, onQueryStart)
	}
	if p.unloadExec == nil {
		return nil, fmt.Errorf("no export executor configured")
	}
	if err := p.unloadExec.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer p.unloadExec.Close()

	status, err := p.unloadExec.CheckQueryStatus(ctx, queryExecutionID)
	if err != nil {
		p.log.Warn("Cannot check query status, restarting export", zap.Error(err))
		return p.Run(ctx, destBucket, onQueryStart)
	}
	p.log.Info("Query status on resume", zap.String("status", status), zap.String("executionID", queryExecutionID))

	switch status {
	case query.QueryStateRunning, query.QueryStateQueued:
		p.log.Info("Reattaching to running query")
		if err := p.unloadExec.WaitForExecution(ctx, queryExecutionID); err != nil {
			return nil, fmt.Errorf("reattached query failed: %w", err)
		}
	case query.QueryStateSucceeded:
		p.log.Info("Query already succeeded, skipping to upload phase")
	default:
		p.log.Info("Query not recoverable, restarting", zap.String("status", status))
		return p.Run(ctx, destBucket, onQueryStart)
	}

	result, err := p.unloadExec.GetExecutionResult(ctx, queryExecutionID)
	if err != nil {
		return nil, fmt.Errorf("get execution result after reattach: %w", err)
	}
	if result.OutputLocation == "" && result.ManifestLocation == "" {
		p.log.Warn("Reattached query has no output location, restarting export")
		return p.Run(ctx, destBucket, onQueryStart)
	}
	unloadOpts := p.unloadOptions()
	return p.runUploadPhase(ctx, destBucket, result, p.useDirectUpload(unloadOpts))
}

func (p *Pipeline) runUploadPhase(
	ctx context.Context,
	destBucket string,
	unloadResult *query.UnloadResult,
	directUpload bool,
) (*PipelineResult, error) {
	if p.uploader == nil {
		return nil, fmt.Errorf("uploader not configured")
	}

	unloadReader, err := p.unloadExec.NewStagingReader(p.config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init staging reader: %w", err)
	}

	var outputFiles []string
	if unloadResult.ManifestLocation != "" {
		outputFiles, err = unloadReader.ParseManifest(ctx, unloadResult.ManifestLocation)
	} else if unloadResult.OutputLocation != "" {
		outputFiles, err = unloadReader.ListFiles(ctx, unloadResult.OutputLocation)
	} else {
		return nil, fmt.Errorf("no UNLOAD manifest or output location")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list UNLOAD output: %w", err)
	}

	if len(outputFiles) == 0 {
		p.log.Warn("UNLOAD produced no files")
		return &PipelineResult{TotalRows: 0}, nil
	}

	exportFormat := normalizedFormat(p.request.Format)
	ext, contentType := formatMeta(exportFormat)
	fileName := exportFileName(p.exportName, p.request.TableName, p.request.DataSetName, ext, p.reportID)
	outputKey := fmt.Sprintf("exports/%s/%s", p.reportID, fileName)

	p.log.Info("Export output file", zap.String("fileName", fileName), zap.String("outputKey", outputKey))

	p.abortOrphanedMultipartUploads(ctx, destBucket)
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
		_ = unloadReader.DeleteFiles(ctx, outputFiles)
		return nil, err
	}

	_ = unloadReader.DeleteFiles(ctx, outputFiles)
	if unloadResult.ManifestLocation != "" {
		_ = unloadReader.DeleteFiles(ctx, []string{unloadResult.ManifestLocation})
	}

	if totalBytes == 0 {
		return &PipelineResult{TotalRows: totalRows, TotalBytes: 0}, nil
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

// abortOrphanedMultipartUploads cleans up incomplete uploads left under this report's
// export prefix by a crashed previous attempt. Best-effort; S3 only — Azure garbage-
// collects uncommitted blocks automatically.
func (p *Pipeline) abortOrphanedMultipartUploads(ctx context.Context, destBucket string) {
	if s3up, ok := p.uploader.(*upload.S3Uploader); ok {
		s3up.AbortIncompleteUploads(ctx, destBucket, fmt.Sprintf("exports/%s/", p.reportID))
	}
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
	})
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
	if p.unloadExec != nil && p.unloadExec.Engine() == query.EngineADX {
		return p.adxUnloadOptions(exportFormat)
	}
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

// adxUnloadOptions picks the native Kusto .export format. Kusto writes comma-separated
// csv or tab-separated tsv only, so any other delimiter falls back to Parquet and the
// local encoder, which honours the requested delimiter.
func (p *Pipeline) adxUnloadOptions(exportFormat string) query.UnloadOptions {
	switch exportFormat {
	case "json":
		return query.UnloadOptions{Format: "json"}
	case "csv":
		delim := p.request.Delimiter
		if delim == "" {
			delim = ","
		}
		switch delim {
		case ",":
			return query.UnloadOptions{Format: "csv", Delimiter: delim}
		case "\t":
			return query.UnloadOptions{Format: "tsv", Delimiter: delim}
		}
		p.log.Warn("CSV export falling back to Parquet .export due to unsupported delimiter",
			zap.String("delimiter", delim))
	case "xlsx", "excel":
		// Excel requires local Parquet conversion.
	default:
		p.log.Warn("Unknown export format, using Parquet .export",
			zap.String("format", p.request.Format))
	}
	return query.UnloadOptions{Format: "parquet"}
}

func (p *Pipeline) useDirectUpload(opts query.UnloadOptions) bool {
	switch opts.Format {
	case "textfile", "json", "csv", "tsv":
		return true
	default:
		return false
	}
}

func (p *Pipeline) processUnloadDirect(ctx context.Context, ur unload.StagingReader, files []string) (int64, int64, error) {
	var header []byte
	if p.request.IncludeHeader && normalizedFormat(p.request.Format) == "csv" {
		columns, err := p.unloadExec.GetQueryColumns(ctx, p.request.Query, p.request.Database)
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
