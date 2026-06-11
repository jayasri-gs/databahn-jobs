package searchExport

import (
	"context"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/consts"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
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

	logging.GetLogger().Info("Starting search export processor",
		zap.Int("parallelism", parallelism))

	db := config.GetDB()
	reports, err := models.GetSearchExportRequests(db)
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

			processExportRequest(ctx, db, r, pipeline.PipelineConfig{
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

func processExportRequest(ctx context.Context, db *gorm.DB, report models.SearchExportReport, cfg pipeline.PipelineConfig) {
	reportID := report.ID.String()

	exportConfig, err := report.GetConfig()
	log := exportLogger(report, exportConfig)

	log.Info("Processing export request", zap.String("name", report.Name))

	if err != nil {
		handleFailure(db, log, reportID, report.Retries, "Failed to parse config: "+err.Error())
		return
	}

	if exportConfig == nil {
		handleFailure(db, log, reportID, report.Retries, "Missing searchExportConfig")
		return
	}

	if err := models.UpdateRequestStatus(db, reportID, consts.PROCESSING); err != nil {
		log.Error("Failed to update status to PROCESSING", zap.Error(err))
		return
	}
	log.Info("Export status updated", zap.String("status", consts.PROCESSING))

	destID, err := uuid.Parse(exportConfig.DestinationID)
	if err != nil {
		handleFailure(db, log, reportID, report.Retries, "Invalid destinationId: "+err.Error())
		return
	}
	tenantID, err := uuid.Parse(report.TenantID)
	if err != nil {
		handleFailure(db, log, reportID, report.Retries, "Invalid tenantId: "+err.Error())
		return
	}

	s3Cfg, err := destination.LoadS3Config(ctx, db, destID, tenantID)
	if err != nil {
		handleFailure(db, log, reportID, report.Retries, "Failed to get destination: "+err.Error())
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

	p := pipeline.New(cfg, reportID, exportConfig, executor, log)
	result, err := p.Run(ctx, s3Cfg.Bucket)
	if err != nil {
		handleFailure(db, log, reportID, report.Retries, err.Error())
		return
	}

	if err := models.UpdateExportComplete(db, reportID, result.PresignedURL, result.Expiry); err != nil {
		handleFailure(db, log, reportID, report.Retries, "Failed to update completion status: "+err.Error())
		return
	}

	log.Info("Export completed successfully",
		zap.String("status", consts.COMPLETED),
		zap.Int64("totalRows", result.TotalRows),
		zap.Int64("totalBytes", result.TotalBytes),
		zap.String("location", result.Location),
		zap.Time("downloadLinkExpiry", result.Expiry))
}

func handleFailure(db *gorm.DB, log *zap.Logger, reportID string, retries int, errMsg string) {
	newRetries := retries + 1
	log.Error("Export failed",
		zap.String("status", consts.FAILED),
		zap.Int("retries", retries),
		zap.Int("newRetries", newRetries),
		zap.String("error", errMsg))

	models.UpdateRequestStatusAndRetries(db, reportID, consts.FAILED, newRetries)
}
