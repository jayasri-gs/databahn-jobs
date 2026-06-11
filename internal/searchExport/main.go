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
		logging.GetLogger().Info("No search export requests found")
		return common.NewJobResultSuccess()
	}

	logging.GetLogger().Info("Found search export requests", zap.Int("count", len(reports)))

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
	logging.GetLogger().Info("Search export processor completed")
	return common.NewJobResultSuccess()
}

func processExportRequest(ctx context.Context, db *gorm.DB, report models.SearchExportReport, cfg pipeline.PipelineConfig) {
	reportID := report.ID.String()

	logging.GetLogger().Info("Processing export request",
		zap.String("reportId", reportID),
		zap.String("name", report.Name))

	if err := models.UpdateRequestStatus(db, reportID, consts.PROCESSING); err != nil {
		logging.GetLogger().Error("Failed to update status", zap.Error(err))
		return
	}

	exportConfig, err := report.GetConfig()
	if err != nil {
		handleFailure(db, reportID, report.Retries, "Failed to parse config: "+err.Error())
		return
	}

	if exportConfig == nil {
		handleFailure(db, reportID, report.Retries, "Missing searchExportConfig")
		return
	}

	destID, err := uuid.Parse(exportConfig.DestinationID)
	if err != nil {
		handleFailure(db, reportID, report.Retries, "Invalid destinationId: "+err.Error())
		return
	}
	tenantID, err := uuid.Parse(report.TenantID)
	if err != nil {
		handleFailure(db, reportID, report.Retries, "Invalid tenantId: "+err.Error())
		return
	}

	s3Cfg, err := destination.LoadS3Config(ctx, db, destID, tenantID)
	if err != nil {
		handleFailure(db, reportID, report.Retries, "Failed to get destination: "+err.Error())
		return
	}

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

	p := pipeline.New(cfg, reportID, exportConfig, executor)
	result, err := p.Run(ctx, s3Cfg.Bucket)
	if err != nil {
		handleFailure(db, reportID, report.Retries, err.Error())
		return
	}

	if err := models.UpdateExportComplete(db, reportID, result.PresignedURL, result.Expiry); err != nil {
		handleFailure(db, reportID, report.Retries, "Failed to update completion status: "+err.Error())
		return
	}

	logging.GetLogger().Info("Export completed successfully",
		zap.String("reportId", reportID),
		zap.Int64("rows", result.TotalRows),
		zap.String("location", result.Location))
}

func handleFailure(db *gorm.DB, reportID string, retries int, errMsg string) {
	logging.GetLogger().Error("Export failed",
		zap.String("reportId", reportID),
		zap.String("error", errMsg))

	newRetries := retries + 1
	models.UpdateRequestStatusAndRetries(db, reportID, consts.FAILED, newRetries)
}
