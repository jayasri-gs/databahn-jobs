package jobs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Default values for environment variables
const (
	DefaultMinReductionThreshold = 0.0
	DefaultWindowOffsetHours     = 1
	DefaultWindowDurationHours   = 3
)

// SendAlertForVCNoReduction generates alerts when volume controller doesn't perform any reduction
func SendAlertForVCNoReduction(ctx context.Context) error {
	// Load configuration from environment variables
	minReductionThreshold := utils.GetEnvInt("VC_MIN_REDUCTION_THRESHOLD", int(DefaultMinReductionThreshold))
	offsetHours := utils.GetEnvInt("VC_WINDOW_OFFSET_HOURS", DefaultWindowOffsetHours)
	windowDurationHours := utils.GetEnvInt("VC_WINDOW_DURATION_HOURS", DefaultWindowDurationHours)

	logger.GetLoggerWithContext(ctx).Info("Volume controller alert configuration loaded",
		zap.Int("min_reduction_threshold", minReductionThreshold),
		zap.Int("window_offset_hours", offsetHours),
		zap.Int("window_duration_hours", windowDurationHours),
	)

	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error getting all tenants", zap.Error(err))
		return err
	}

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error getting alerts manager", zap.Error(err))
		return err
	}

	defer func() {
		alertsManager.Close(ctx)
	}()

	// Calculate time ranges using configured offset and window duration
	// endTime = now - offsetHours; startTime = endTime - windowDurationHours
	now := time.Now().UTC()
	endTime := now.Add(-time.Duration(offsetHours) * time.Hour)
	startTime := endTime.Add(-time.Duration(windowDurationHours) * time.Hour)

	// Note: configured via VC_WINDOW_OFFSET_HOURS and VC_WINDOW_DURATION_HOURS

	for _, t := range tenants {
		tenantId := t.Id.String()
		logger.GetLoggerWithContext(ctx).Info("checking pipelines for tenant", zap.String("tenant_id", tenantId))

		// Get all active pipelines with their source and destination mappings for this tenant
		pipelines, err := pipeline.GetActivePipelinesWithMappings(ctx, db, t.Id)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("Error getting active pipelines for tenant",
				zap.Error(err), zap.String("tenant_id", tenantId))
			continue
		}

		if len(pipelines) == 0 {
			logger.GetLoggerWithContext(ctx).Info("No active pipelines found for tenant", zap.String("tenant_id", tenantId))
			continue
		}

		for _, pipelineMapping := range pipelines {
			pipelineId := pipelineMapping.Pipeline.ID.String()
			sourceName := pipelineMapping.SourceName
			destinationName := pipelineMapping.DestinationName

			logger.GetLoggerWithContext(ctx).Info("checking pipeline for volume controller reduction",
				zap.String("tenant_id", tenantId),
				zap.String("pipeline_id", pipelineId),
				zap.String("source", sourceName),
				zap.String("destination", destinationName))

			// Check if this pipeline has active volume control rules
			hasActiveRules, err := pipeline.HasActiveRules(ctx, db, pipelineMapping.Pipeline.ID, t.Id)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error checking active rules for pipeline",
					zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
				continue
			}

			if !hasActiveRules {
				logger.GetLoggerWithContext(ctx).Info("No active volume control rules for pipeline, skipping",
					zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
				continue
			}

			// Get source data plane ID
			sourceDataPlaneID, err := getSourceDataPlaneID(ctx, db, pipelineMapping.LogSourceID)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error getting source data plane ID",
					zap.Error(err), zap.String("tenant_id", tenantId), zap.String("source_id", pipelineMapping.LogSourceID.String()))
				continue
			}

			// Get volume controller reduction statistics for this source and destination
			totalIngested, totalDelivered, reductionPercent, err := getPipelineVolumeControllerStats(
				ctx, t.Id, pipelineMapping.LogSourceID, pipelineMapping.DestinationID, startTime, endTime)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error getting pipeline volume controller stats",
					zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
				continue
			}

			logger.GetLoggerWithContext(ctx).Info("Pipeline volume controller stats",
				zap.String("tenant_id", tenantId),
				zap.String("pipeline_id", pipelineId),
				zap.String("source", sourceName),
				zap.String("destination", destinationName),
				zap.Float64("total_ingested", totalIngested),
				zap.Float64("total_delivered", totalDelivered),
				zap.Float64("reduction_percent", reductionPercent))

			// Check if volume controller is not performing adequate reduction
			if shouldAlert(totalIngested, totalDelivered, reductionPercent, float64(minReductionThreshold)) {
				logger.GetLoggerWithContext(ctx).Info("Pipeline volume controller not performing adequate reduction, sending alert",
					zap.String("tenant_id", tenantId),
					zap.String("pipeline_id", pipelineId),
					zap.String("source", sourceName),
					zap.String("destination", destinationName),
					zap.Float64("reduction_percent", reductionPercent))

				vcAlert := model.NewVCNoReductionAlert(&t, &pipelineMapping, sourceDataPlaneID,
					totalIngested, totalDelivered, reductionPercent, startTime, endTime)

				alert, err := buildVCNoReductionAlert(*vcAlert, float64(minReductionThreshold))
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("Error building VC no reduction alert",
						zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
					continue
				}

				err = alertsManager.SendAlerts([]*alerts_async.Alert{alert})
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("Error sending VC no reduction alert",
						zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
					continue
				}

				logger.GetLoggerWithContext(ctx).Info("Successfully sent VC no reduction alert",
					zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
			} else {
				logger.GetLoggerWithContext(ctx).Info("Pipeline volume controller performing adequate reduction, no alert needed",
					zap.String("tenant_id", tenantId),
					zap.String("pipeline_id", pipelineId),
					zap.String("source", sourceName),
					zap.String("destination", destinationName),
					zap.Float64("reduction_percent", reductionPercent))
			}
		}
	}

	return nil
}

// getPipelineVolumeControllerStats gets ingestion and delivery stats for a specific pipeline (source to destination)
func getPipelineVolumeControllerStats(ctx context.Context, tenantId, sourceId, destinationId uuid.UUID, startTime, endTime time.Time) (float64, float64, float64, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)

	// Get ingestion stats for this specific source
	ingestionQuery := fmt.Sprintf("tags.component_name: \"ingestion\" AND name: \"total_events_delivered\" AND tags.db_event_source_id: \"%s\"", sourceId.String())
	ingestionResponse, err := statistics.GetStatsSum(ctx, ingestionQuery, tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error getting ingestion stats for source %s: %w", sourceId.String(), err)
	}

	// Get delivery stats for this specific destination from the source
	deliveryQuery := fmt.Sprintf("tags.component_name: \"dispenser\" AND name: \"total_events_delivered\" AND tags.db_event_source_id: \"%s\" AND tags.destination_id: \"%s\"", sourceId.String(), destinationId.String())
	deliveryResponse, err := statistics.GetStatsSum(ctx, deliveryQuery, tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error getting delivery stats for source %s to destination %s: %w", sourceId.String(), destinationId.String(), err)
	}

	totalIngested := ingestionResponse.Sum
	totalDelivered := deliveryResponse.Sum

	// Calculate reduction percentage
	var reductionPercent float64
	if totalIngested > 0 {
		reductionPercent = (1 - (totalDelivered / totalIngested)) * 100
		if reductionPercent < 0 {
			reductionPercent = 0 // Ensure we don't have negative reduction
		}
	}

	return totalIngested, totalDelivered, reductionPercent, nil
}

// getSourceDataPlaneID gets the data plane ID for a source
func getSourceDataPlaneID(ctx context.Context, db *gorm.DB, sourceId uuid.UUID) (uuid.UUID, error) {
	var src source.Source
	err := db.WithContext(ctx).Where("id = ?", sourceId).First(&src).Error
	if err != nil {
		return uuid.Nil, fmt.Errorf("error getting source data plane ID: %w", err)
	}
	return src.DataPlaneId, nil
}

// shouldAlert determines if an alert should be sent based on volume controller performance
func shouldAlert(totalIngested, totalDelivered, reductionPercent, minReductionThreshold float64) bool {
	// Only alert if there's significant traffic but low reduction
	// Minimum threshold of 1000 events to avoid noise from low-traffic tenants
	if totalIngested < 1000 {
		return false
	}

	// Alert if reduction is exactly 0% (no reduction)
	return reductionPercent <= minReductionThreshold
}

// buildVCNoReductionAlert builds an alert for volume controller with no reduction
func buildVCNoReductionAlert(vcAlert model.VCNoReductionAlert, minReductionThreshold float64) (*alerts_async.Alert, error) {
	startStr := vcAlert.CheckStartTime.UTC().Format(time.RFC3339)
	endStr := vcAlert.CheckEndTime.UTC().Format(time.RFC3339)

	var title, message string

	if vcAlert.ReductionPercent <= 0 {
		title = "Volume Controller Alert: No Data Reduction Detected"
		message = fmt.Sprintf(
			"Volume controller is not performing any data reduction for pipeline '%s'. "+
				"Between %s and %s, %.0f events were ingested from source '%s' and %.0f events were delivered to destination '%s' "+
				"(%.2f%% reduction). This may indicate volume controller rules are not functioning properly.",
			vcAlert.Pipeline.Pipeline.Name,
			startStr,
			endStr,
			vcAlert.TotalIngested,
			vcAlert.SourceName,
			vcAlert.TotalDelivered,
			vcAlert.DestinationName,
			vcAlert.ReductionPercent,
		)
	} else {
		title = fmt.Sprintf("Volume Controller Alert: Low Data Reduction (%.1f%%)", vcAlert.ReductionPercent)
		message = fmt.Sprintf(
			"Volume controller is performing minimal data reduction for pipeline '%s'. "+
				"Between %s and %s, %.0f events were ingested from source '%s' and %.0f events were delivered to destination '%s' "+
				"(%.2f%% reduction). Expected reduction should be at least %.0f%%.",
			vcAlert.Pipeline.Pipeline.Name,
			startStr,
			endStr,
			vcAlert.TotalIngested,
			vcAlert.SourceName,
			vcAlert.TotalDelivered,
			vcAlert.DestinationName,
			vcAlert.ReductionPercent,
			minReductionThreshold,
		)
	}

	return alerts_async.NewAlert(
		alerts_async.VolumeControlRule,
		alerts_async.WithEntity(vcAlert),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.VolumeDeviationChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10005, "Volume controller not performing reduction"),
	)
}
