package searchExport

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/consts"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/state"
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
	efsMountPath := utils.GetEnvOrDefault("SEARCH_EXPORT_EFS_MOUNT", "/mnt/efs/search-export")
	staleMinutes := utils.GetEnvInt("SEARCH_EXPORT_STALE_PROCESSING_MINUTES", 40)
	staleCutoff := time.Now().Add(-time.Duration(staleMinutes) * time.Minute)

	logging.GetLogger().Info("Starting search export processor",
		zap.Int("parallelism", parallelism),
		zap.String("efsMountPath", efsMountPath),
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
				EFSMountPath:     efsMountPath,
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
		handleFailure(ctx, db, log, cfg, report, "Failed to parse config: "+err.Error(), nil)
		return
	}
	if exportConfig == nil {
		handleFailure(ctx, db, log, cfg, report, "Missing searchExportConfig", nil)
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

	destID, err := uuid.Parse(exportConfig.DestinationID)
	if err != nil {
		handleFailure(ctx, db, log, cfg, report, "Invalid destinationId: "+err.Error(), nil)
		return
	}
	tenantID, err := uuid.Parse(report.TenantID)
	if err != nil {
		handleFailure(ctx, db, log, cfg, report, "Invalid tenantId: "+err.Error(), nil)
		return
	}

	s3Cfg, err := destination.LoadS3Config(ctx, db, destID, tenantID)
	if err != nil {
		handleFailure(ctx, db, log, cfg, report, "Failed to get destination: "+err.Error(), nil)
		return
	}
	log.Info("Destination config loaded",
		zap.String("bucket", s3Cfg.Bucket),
		zap.String("region", s3Cfg.Region),
		zap.String("authType", s3Cfg.AuthType),
		zap.String("athenaOutputLocation", s3Cfg.AthenaOutputLocation()))

	athenaConfig := query.AthenaConfig{
		Region:          s3Cfg.Region,
		Workgroup:       "primary",
		OutputLocation:  s3Cfg.AthenaOutputLocation(),
		AuthType:        s3Cfg.AuthType,
		AccessKeyID:     s3Cfg.AccessKeyID,
		SecretAccessKey: s3Cfg.SecretAccessKey,
		RoleArn:         s3Cfg.RoleArn,
		ExternalID:      s3Cfg.ExternalID,
	}
	executor := query.NewAthenaExecutor(athenaConfig)
	executor.SetLogger(log)

	p := pipeline.New(cfg, reportID, report.Name, exportConfig, executor, log)

	onAthenaStart := func(executionID string) error {
		return models.UpdateAthenaExecutionID(db, reportID, executionID)
	}

	var result *pipeline.PipelineResult
	if isStaleProcessing {
		result, err = p.ResumeRun(ctx, s3Cfg.Bucket, onAthenaStart)
	} else {
		result, err = p.Run(ctx, s3Cfg.Bucket, onAthenaStart)
	}
	if err != nil {
		var awsCfg *aws.Config
		if c, ok := executor.GetAWSConfig().(aws.Config); ok {
			awsCfg = &c
		}
		handleFailure(ctx, db, log, cfg, report, err.Error(), awsCfg)
		return
	}

	if err := models.UpdateExportComplete(db, reportID, result.PresignedURL, result.Expiry); err != nil {
		var awsCfg *aws.Config
		if c, ok := executor.GetAWSConfig().(aws.Config); ok {
			awsCfg = &c
		}
		handleFailure(ctx, db, log, cfg, report, "Failed to update completion status: "+err.Error(), awsCfg)
		return
	}

	log.Info("Export completed successfully",
		zap.String("status", consts.COMPLETED),
		zap.Int64("totalRows", result.TotalRows),
		zap.Int64("totalBytes", result.TotalBytes),
		zap.String("location", result.Location),
		zap.Time("downloadLinkExpiry", result.Expiry))
}

func handleFailure(ctx context.Context, db *gorm.DB, log *zap.Logger, cfg pipeline.PipelineConfig, report models.SearchExportReport, errMsg string, awsCfg *aws.Config) {
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

	if newRetries >= consts.MaxRetries && cfg.EFSMountPath != "" {
		cp, err := state.Read(cfg.EFSMountPath, report.ID.String())
		if err == nil && cp != nil && cp.UploadID != "" && awsCfg != nil {
			if abortErr := upload.AbortOrphanedUpload(ctx, *awsCfg, cp.Bucket, cp.Key, cp.UploadID); abortErr != nil {
				log.Warn("Failed to abort orphaned multipart upload", zap.Error(abortErr))
			}
		}
		if err := state.Delete(cfg.EFSMountPath, report.ID.String()); err != nil {
			log.Warn("Failed to delete EFS checkpoint on permanent failure", zap.Error(err))
		}
	}
}
