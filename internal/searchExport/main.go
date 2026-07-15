package searchExport

import (
	"context"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/consts"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/factory"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func GenerateSearchExport(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	parallelism := utils.GetEnvInt("SEARCH_EXPORT_PARALLELISM", 2)
	tempDir := utils.GetEnvOrDefault("SEARCH_EXPORT_TEMP_DIR", "/tmp/search-export")
	maxSegmentMB := utils.GetEnvInt("SEARCH_EXPORT_MAX_SEGMENT_MB", 10)
	presignHours := utils.GetEnvInt("SEARCH_EXPORT_PRESIGN_HOURS", consts.PresignExpiryHours)
	staleMinutes := utils.GetEnvInt("SEARCH_EXPORT_STALE_PROCESSING_MINUTES", 40)
	staleCutoff := time.Now().Add(-time.Duration(staleMinutes) * time.Minute)

	logging.GetLogger().Info("Starting search export processor",
		zap.Int("parallelism", parallelism),
		zap.Int("staleMinutes", staleMinutes))

	db := config.GetDB()
	reports, err := models.GetSearchExportRequests(db, staleCutoff)
	if err != nil {
		jobErrors = append(jobErrors, common.JobError{Message: err.Error()})
		return common.NewJobResultFromErrors(jobErrors)
	}

	if len(reports) == 0 {
		logging.GetLogger().Info("No search export requests found during poll")
		return common.NewJobResultSuccess()
	}

	reportIDs := make([]string, len(reports))
	for i, r := range reports {
		reportIDs[i] = r.ID.String()
	}
	logging.GetLogger().Info("Polled search export requests",
		zap.Int("count", len(reports)),
		zap.Strings("reportIds", reportIDs))

	var wg sync.WaitGroup
	sem := make(chan struct{}, parallelism)

	for _, report := range reports {
		wg.Add(1)
		sem <- struct{}{}

		go func(r models.SearchExportReport) {
			defer wg.Done()
			defer func() { <-sem }()

			processExportRequest(ctx, db, r, staleCutoff, pipeline.PipelineConfig{
				TempDir:          tempDir,
				MaxSegmentSizeMB: maxSegmentMB,
				PresignExpiry:    time.Duration(presignHours) * time.Hour,
				LifecycleTag:     "export-expiry=true",
			})
		}(report)
	}

	wg.Wait()
	logging.GetLogger().Info("Search export processor completed",
		zap.Int("processedCount", len(reports)))
	return common.NewJobResultSuccess()
}

func processExportRequest(ctx context.Context, db *gorm.DB, report models.SearchExportReport, staleCutoff time.Time, cfg pipeline.PipelineConfig) {
	reportID := report.ID.String()
	exportConfig, err := report.GetConfig()
	log := exportLogger(report, exportConfig)

	if err != nil {
		handleFailure(ctx, db, log, report, "Failed to parse config: "+err.Error(), "", nil, nil)
		return
	}
	if exportConfig == nil {
		handleFailure(ctx, db, log, report, "Missing searchExportConfig", "", nil, nil)
		return
	}

	isStaleProcessing := report.Status == consts.PROCESSING
	if isStaleProcessing {
		claimed, err := models.ClaimStaleProcessingJob(db, reportID, staleCutoff)
		if err != nil {
			log.Error("Failed to claim stale PROCESSING job", zap.Error(err))
			return
		}
		if !claimed {
			log.Info("Stale PROCESSING job already claimed by another pod — skipping")
			return
		}
		log.Info("Claimed stale PROCESSING job for resume")
	} else {
		if err := models.UpdateRequestStatus(db, reportID, consts.PROCESSING); err != nil {
			log.Error("Failed to update status to PROCESSING", zap.Error(err))
			return
		}
		now := time.Now()
		if err := models.UpdateExecutionStartedAt(db, reportID, now); err != nil {
			log.Error("Failed to write executionStartedAt — aborting to avoid duplicate processing", zap.Error(err))
			return
		}
	}

	log.Info("Export status updated", zap.String("status", consts.PROCESSING))

	tenantID, err := uuid.Parse(report.TenantID)
	if err != nil {
		handleFailure(ctx, db, log, report, "Invalid tenantId: "+err.Error(), "", nil, nil)
		return
	}

	deps, err := factory.NewExportDeps(ctx, db, exportConfig, tenantID, reportID, log)
	if err != nil {
		handleFailure(ctx, db, log, report, "Failed to build export dependencies: "+err.Error(), "", nil, nil)
		return
	}

	log.Info("Export dependencies loaded",
		zap.String("exportBucket", deps.ExportBucket),
		zap.String("engine", deps.QueryEngine),
		zap.Bool("legacyMode", deps.LegacyMode))

	p := pipeline.New(cfg, reportID, report.Name, exportConfig, deps.Athena, deps.Synapse, deps.Uploader, deps.StagingBlobConfig, log)

	onQueryStart := func(executionID string) error {
		return models.UpdateQueryExecutionID(db, reportID, executionID)
	}

	// stagingCleanup drops the report's CETAS objects and staged blobs on permanent
	// failure. Reconnects because the pipeline closes its Synapse connection on exit.
	stagingCleanup := func(cleanupCtx context.Context) {
		ce, ok := deps.Synapse.(query.CETASExecutor)
		if !ok || deps.StagingBlobConfig == nil {
			return
		}
		if err := deps.Synapse.Connect(cleanupCtx); err != nil {
			log.Warn("CETAS permanent-failure cleanup: synapse connect failed", zap.Error(err))
			return
		}
		defer deps.Synapse.Close()
		blobClient, err := destination.NewAzureBlobClient(deps.StagingBlobConfig)
		if err != nil {
			log.Warn("CETAS permanent-failure cleanup: blob client failed", zap.Error(err))
			return
		}
		reader, err := unload.NewBlobReader(blobClient, deps.StagingBlobConfig.Container, cfg.TempDir)
		if err != nil {
			log.Warn("CETAS permanent-failure cleanup: blob reader failed", zap.Error(err))
			return
		}
		pipeline.CleanupCETASArtifacts(cleanupCtx, ce, reader, reportID, log)
	}

	var result *pipeline.PipelineResult
	if isStaleProcessing {
		result, err = p.ResumeRun(ctx, deps.ExportBucket, exportConfig.QueryExecutionID, onQueryStart)
	} else {
		result, err = p.Run(ctx, deps.ExportBucket, onQueryStart)
	}
	if err != nil {
		handleFailure(ctx, db, log, report, err.Error(), deps.ExportBucket, deps.Uploader, stagingCleanup)
		return
	}

	if err := models.UpdateExportComplete(db, reportID, result.PresignedURL, result.Expiry); err != nil {
		handleFailure(ctx, db, log, report, "Failed to update completion status: "+err.Error(), deps.ExportBucket, deps.Uploader, stagingCleanup)
		return
	}

	log.Info("Export completed successfully",
		zap.String("status", consts.COMPLETED),
		zap.Int64("totalRows", result.TotalRows),
		zap.Int64("totalBytes", result.TotalBytes),
		zap.String("location", result.Location),
		zap.Time("downloadLinkExpiry", result.Expiry))
}

func handleFailure(ctx context.Context, db *gorm.DB, log *zap.Logger, report models.SearchExportReport, errMsg string, exportBucket string, uploader upload.CloudUploader, stagingCleanup func(context.Context)) {
	newRetries := report.Retries + 1
	log.Error("Export failed",
		zap.String("status", consts.FAILED),
		zap.Int("retries", report.Retries),
		zap.Int("newRetries", newRetries),
		zap.String("error", errMsg))

	if err := models.UpdateRequestStatusAndRetries(db, report.ID.String(), consts.FAILED, newRetries); err != nil {
		log.Error("Failed to update status and retries after export failure", zap.Error(err))
		return
	}

	if newRetries >= consts.MaxRetries {
		if s3up, ok := uploader.(*upload.S3Uploader); ok && exportBucket != "" {
			s3up.AbortIncompleteUploads(ctx, exportBucket, "exports/"+report.ID.String()+"/")
		}
		if stagingCleanup != nil {
			stagingCleanup(ctx)
		}
	}
}
