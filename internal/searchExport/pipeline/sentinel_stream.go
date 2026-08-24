package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"go.uber.org/zap"
)

// runSentinelStreamExport pulls one Sentinel result set over the query API, encodes it, and
// uploads it to the export destination.
//
// Simpler than the Synapse row-stream path: there is no partition filter to apply and no
// cursor to carry, because the KQL arrives fully planned from backend-service and the
// request plan lives in query.PlanSentinelQueries. Columns arrive with the response schema
// rather than from a preflight query.
func (p *Pipeline) runSentinelStreamExport(ctx context.Context, destBucket string, exec query.RowStreamExecutor) (*PipelineResult, error) {
	p.log.Info("Running Sentinel query-API stream export")

	if err := exec.ValidateExportQuery(ctx, p.request.Query); err != nil {
		return nil, err
	}

	opts := query.SentinelStreamOptionsFromEnv()
	var columns []string
	opts.OnColumns = func(cols []string) error {
		columns = cols
		return nil
	}

	// All validation must happen before uploader.Init — an early return after Init would
	// leave an orphaned multipart upload behind.
	exportFormat := normalizedFormat(p.request.Format)
	ext, contentType := formatMeta(exportFormat)
	fileName, outputKey := p.buildExportObjectKey(ext)
	p.log.Info("Export output file", zap.String("fileName", fileName), zap.String("outputKey", outputKey))

	p.abortOrphanedMultipartUploads(ctx, destBucket)
	if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
		return nil, fmt.Errorf("failed to init uploader: %w", err)
	}

	started := time.Now()
	totalRows, totalBytes, err := p.encodeRowsToUploader(ctx,
		func() []string { return columns },
		func(cb func([]interface{}) error) error {
			_, streamErr := exec.StreamRows(ctx, p.request.Query, opts, cb)
			return streamErr
		})
	if err != nil {
		_ = p.uploader.Abort(ctx)
		return nil, fmt.Errorf("sentinel stream export: %w", err)
	}

	p.log.Info("Sentinel export streamed",
		zap.Int64("totalRows", totalRows),
		zap.Int64("totalBytes", totalBytes),
		zap.Duration("elapsed", time.Since(started)))

	if totalBytes == 0 {
		return &PipelineResult{TotalRows: totalRows, TotalBytes: 0}, nil
	}

	// Not a warning: processExportRequest stores this as the report's download_link, so
	// returning an empty one marks the export COMPLETED with nothing to download. Failing
	// instead lets the job retry, and the retry overwrites the same object key.
	presignedURL, err := p.uploader.GeneratePresignedURL(ctx, p.config.PresignExpiry)
	if err != nil {
		return nil, fmt.Errorf("generate download URL for completed export: %w", err)
	}

	return &PipelineResult{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		Location:     p.uploader.GetLocation(),
		PresignedURL: presignedURL,
		Expiry:       time.Now().Add(p.config.PresignExpiry),
	}, nil
}
