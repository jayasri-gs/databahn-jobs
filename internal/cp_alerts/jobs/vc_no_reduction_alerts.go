package jobs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/vc_rule"

	"github.com/databahn-ai/databahn-jobs/internal/util"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
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

	osClient := os.GetClient()

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

		// Track healthy source-destination pairs (where no alert is needed), so we can auto-resolve existing open alerts
		type srcDstPair struct{ srcId, dstId string }
		var healthyPairs []srcDstPair

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
			hasActiveRules, vcRuleCount, err := pipeline.HasActiveRules(ctx, db, pipelineMapping.Pipeline.ID, t.Id)
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

			if vcRuleCount == 1 {
				rules, err := vc_rule.GetActiveVCRulesByPipelineAndTenant(pipelineMapping.Pipeline.ID, t.Id, db)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("Error getting active rules for pipeline",
						zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
					continue
				}

				if len(rules) == 1 {
					rule := rules[0]
					filter, err := rule.GetRuleFilters()
					if err != nil {
						logger.GetLoggerWithContext(ctx).Error("Error parsing rule filters",
							zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId), zap.String("rule_id", rule.ID.String()))
						continue
					}

					// Check if filter is nil (empty JSON or parsing issues)
					if filter == nil {
						logger.GetLoggerWithContext(ctx).Warn("Rule filters is nil, proceeding with VC reduction check",
							zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId), zap.String("rule_id", rule.ID.String()))
						// Don't continue here - we want to proceed with the VC reduction check
					} else if filter.Schema == "v1" {
						// Check if Filter config exists and is valid
						if len(filter.Filter.Rules) >= 1 && strings.TrimSpace(strings.ToLower(filter.Filter.Combinator)) == "and" {
							// Safe to access first rule since we verified length
							firstRule := filter.Filter.Rules[0]
							if len(filter.Filter.Rules) == 1 && firstRule.Field == "rawevent" && firstRule.Operator == "notNull" && firstRule.Value == "" {
								logger.GetLoggerWithContext(ctx).Info("Only a single 'rawevent notNull' rule present, skipping VC reduction check",
									zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId), zap.String("rule_id", rule.ID.String()))
								continue
							}
						}
					} else {
						logger.GetLoggerWithContext(ctx).Debug("Rule filter schema is not v1, proceeding with VC reduction check",
							zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId), zap.String("rule_id", rule.ID.String()), zap.String("schema", filter.Schema))
					}
				}
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

			shouldAlert, shouldResolve := shouldAlertOrResolve(totalIngested, totalDelivered, reductionPercent, float64(minReductionThreshold))

			// Check if volume controller is not performing adequate reduction
			if shouldAlert {
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
			}

			if shouldResolve {
				logger.GetLoggerWithContext(ctx).Info("Pipeline volume controller performing adequate reduction, no alert needed",
					zap.String("tenant_id", tenantId),
					zap.String("pipeline_id", pipelineId),
					zap.String("source", sourceName),
					zap.String("destination", destinationName),
					zap.Float64("reduction_percent", reductionPercent))
				healthyPairs = append(healthyPairs, srcDstPair{srcId: pipelineMapping.LogSourceID.String(), dstId: pipelineMapping.DestinationID.String()})
			}
		}

		// Auto-resolve any open VC alerts for healthy source-destination pairs
		if len(healthyPairs) > 0 {
			var alertsToDismiss []string
			for _, pair := range healthyPairs {
				q := fmt.Sprintf("tenantId:%s AND dismissed:false AND functionalityType:%s AND functionalityEntityId:%s AND secondaryEntityId:%s",
					tenantId, alerts_async.VolumeDeviationChecker.String(), pair.srcId, pair.dstId)
				openAlerts, _, err := os.Search(ctx, osClient, common.AlertsIndex, q)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error while searching VC alerts to auto-resolve", zap.Error(err), zap.String("query", q))
					continue
				}
				alerts, err := statistics.ParseAlertDocuments(openAlerts)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error while decoding OpenSearch VC alert response", zap.Error(err), zap.String("tenantId", tenantId))
					continue
				}
				for _, alrt := range alerts {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
			if len(alertsToDismiss) > 0 {
				if err := alertsManager.AutoResolveAlerts(alertsToDismiss); err != nil {
					logger.GetLoggerWithContext(ctx).Error("error while auto-resolving VC alerts", zap.Error(err), zap.String("tenant_id", tenantId))
				} else {
					logger.GetLoggerWithContext(ctx).Info("auto-resolved VC alerts for healthy pipelines", zap.String("tenant_id", tenantId), zap.Any("alertsToDismiss", alertsToDismiss))
				}
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

// shouldAlertOrResolve determines if an alert should be sent based on volume controller performance
func shouldAlertOrResolve(totalIngested, totalDelivered, reductionPercent, minReductionThreshold float64) (bool, bool) {
	// Only alert if there's significant traffic but low reduction
	// Minimum threshold of 1000 events to avoid noise from low-traffic tenants
	if totalIngested < 1000 {
		return false, false
	}

	return reductionPercent <= minReductionThreshold, !(reductionPercent <= minReductionThreshold)
}

// buildVCNoReductionAlert builds an alert for volume controller with no reduction
func buildVCNoReductionAlert(vcAlert model.VCNoReductionAlert, minReductionThreshold float64) (*alerts_async.Alert, error) {
	var title, message string
	pipelineName := vcAlert.Pipeline.Pipeline.Name

	if vcAlert.ReductionPercent <= 0 {
		title = fmt.Sprintf("Volume Controller Alert: No Data Reduction Detected for Pipeline '%s'", pipelineName)
		message = fmt.Sprintf(
			"Volume controller is not performing any data reduction for pipeline '%s'. "+
				"As of '%s', between '%s' and '%s', %s events were ingested from source '%s' and %s events were delivered to destination '%s' "+
				"(%.2f%% reduction). This may indicate volume controller rules are not performing well.",
			vcAlert.Pipeline.Pipeline.Name,
			util.HumanReadableTimeWithZone(time.Now()),
			util.HumanReadableTimeWithZone(vcAlert.CheckStartTime),
			util.HumanReadableTimeWithZone(vcAlert.CheckEndTime),
			util.HumanReadableNumber(int64(vcAlert.TotalIngested)),
			vcAlert.SourceName,
			util.HumanReadableNumber(int64(vcAlert.TotalDelivered)),
			vcAlert.DestinationName,
			vcAlert.ReductionPercent,
		)
	} else {
		title = fmt.Sprintf("Volume Controller Alert: Low Data Reduction (%.2f%%) for Pipeline '%s'", vcAlert.ReductionPercent, pipelineName)
		message = fmt.Sprintf(
			"Volume controller is performing minimal data reduction for pipeline '%s'. "+
				"As of '%s', between '%s' and '%s', %s events were ingested from source '%s' and %s events were delivered to destination '%s' "+
				"(%.2f%% reduction). This may indicate volume controller rules are not performing well.",
			vcAlert.Pipeline.Pipeline.Name,
			util.HumanReadableTimeWithZone(time.Now()),
			util.HumanReadableTimeWithZone(vcAlert.CheckStartTime),
			util.HumanReadableTimeWithZone(vcAlert.CheckEndTime),
			util.HumanReadableNumber(int64(vcAlert.TotalIngested)),
			vcAlert.SourceName,
			util.HumanReadableNumber(int64(vcAlert.TotalDelivered)),
			vcAlert.DestinationName,
			vcAlert.ReductionPercent,
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
