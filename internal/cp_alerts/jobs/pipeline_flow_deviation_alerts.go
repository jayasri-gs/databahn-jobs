package jobs

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/data_transformation"
	"github.com/databahn-ai/databahn-jobs/internal/store/enrichment"

	"github.com/databahn-ai/databahn-jobs/internal/store/vc_rule"
	"github.com/databahn-ai/databahn-jobs/internal/util"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/constants"
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
	DefaultDeviationThreshold          = 10.0 // 10% deviation threshold
	DefaultPipelineWindowOffsetHours   = 1
	DefaultPipelineWindowDurationHours = 24
	DefaultMinEventsThreshold          = 1000 // Minimum events to consider for alerting
)

// Component names used in statistics
const (
	ComponentIngestion      = "ingestion"
	ComponentParsing        = "parser"
	ComponentTransformation = "transformation"
	ComponentEnrichment     = "enrichment"
	ComponentRuleEngine     = "rule-engine"
	ComponentDispenser      = "dispenser"
)

// SendAlertForPipelineFlowDeviation generates alerts when there are significant deviations in pipeline event flow
func SendAlertForPipelineFlowDeviation(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	// Load configuration from environment variables
	deviationThreshold := utils.GetEnvFloat("PIPELINE_DEVIATION_THRESHOLD", DefaultDeviationThreshold)
	offsetHours := utils.GetEnvInt("PIPELINE_WINDOW_OFFSET_HOURS", DefaultPipelineWindowOffsetHours)
	windowDurationHours := utils.GetEnvInt("PIPELINE_WINDOW_DURATION_HOURS", DefaultPipelineWindowDurationHours)
	minEventsThreshold := utils.GetEnvInt("PIPELINE_MIN_EVENTS_THRESHOLD", DefaultMinEventsThreshold)

	logger.GetLoggerWithContext(ctx).Info("Pipeline flow deviation alert configuration loaded",
		zap.Float64("deviation_threshold", deviationThreshold),
		zap.Int("window_offset_hours", offsetHours),
		zap.Int("window_duration_hours", windowDurationHours),
		zap.Int("min_events_threshold", minEventsThreshold),
	)

	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		errorMsg := fmt.Sprintf("error getting all tenants: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("Error getting all tenants", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error getting alerts manager: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLoggerWithContext(ctx).Error("Error getting alerts manager", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	defer func() {
		alertsManager.Close(ctx)
	}()

	osClient := os.GetClient()

	// Calculate time ranges using configured offset and window duration
	now := time.Now().UTC()
	endTime := now.Add(-time.Duration(offsetHours) * time.Hour)
	startTime := endTime.Add(-time.Duration(windowDurationHours) * time.Hour)

	for _, t := range tenants {
		tenantId := t.Id.String()
		logger.GetLoggerWithContext(ctx).Info("checking pipelines for tenant", zap.String("tenant_id", tenantId))

		// Cache active sources for this tenant
		activeSources, err := source.GetSourcesByTenantAndStatus(ctx, db, t.Id, "ACTIVE")
		if err != nil {
			errorMsg := fmt.Sprintf("error getting active sources for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLoggerWithContext(ctx).Error("Error getting active sources for tenant",
				zap.Error(err), zap.String("tenant_id", tenantId))
			continue
		}

		if len(activeSources) == 0 {
			logger.GetLoggerWithContext(ctx).Info("No active sources found for tenant", zap.String("tenant_id", tenantId))
			continue
		}

		// Cache active sources by ID for quick lookup
		activeSourcesMap := make(map[uuid.UUID]source.Source)
		for _, src := range activeSources {
			activeSourcesMap[src.ID] = src
		}

		// Get all active pipelines with their source and destination mappings for this tenant
		pipelines, err := pipeline.GetActivePipelinesWithMappings(ctx, db, t.Id)
		if err != nil {
			errorMsg := fmt.Sprintf("error getting active pipelines for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLoggerWithContext(ctx).Error("Error getting active pipelines for tenant",
				zap.Error(err), zap.String("tenant_id", tenantId))
			continue
		}

		if len(pipelines) == 0 {
			logger.GetLoggerWithContext(ctx).Info("No active pipelines found for tenant", zap.String("tenant_id", tenantId))
			continue
		}

		// Track healthy pipeline stages for auto-resolution
		type pipelineStageKey struct{ pipelineId, sourceId, stageId string }
		var healthyStages []pipelineStageKey

		for _, pipelineMapping := range pipelines {
			pipelineId := pipelineMapping.Pipeline.ID.String()
			sourceName := pipelineMapping.SourceName
			destinationName := pipelineMapping.DestinationName
			if pipelineMapping.Pipeline.Status != "ACTIVE" {
				logger.GetLoggerWithContext(ctx).Debug("Skipping inactive pipeline",
					zap.String("tenant_id", tenantId),
					zap.String("pipeline_id", pipelineId),
					zap.String("status", pipelineMapping.Pipeline.Status))
				continue
			}

			// Check if this source is in active sources cache
			_, isSourceActive := activeSourcesMap[pipelineMapping.LogSourceID]
			if !isSourceActive {
				logger.GetLoggerWithContext(ctx).Debug("Source not in active sources cache, skipping",
					zap.String("tenant_id", tenantId),
					zap.String("pipeline_id", pipelineId),
					zap.String("source_id", pipelineMapping.LogSourceID.String()))
				continue
			}

			// Skip alert if pipeline sends to Databahn Sandbox destination and tenant has disabled sandbox alerts
			if pipelineMapping.DestinationID.String() == constants.SandboxDestinationID {
				shouldSkip, err := util.ShouldSkipSandboxAlerts(db, t.Id)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error checking sandbox alerts config, skipping sandbox alerts as fail-safe",
						zap.Error(err),
						zap.String("tenant_id", tenantId),
						zap.String("pipeline_id", pipelineId))
					continue
				}
				if shouldSkip {
					logger.GetLoggerWithContext(ctx).Info("skipping pipeline flow deviation check for sandbox pipeline (tenant has disabled sandbox alerts)",
						zap.String("tenant_id", tenantId),
						zap.String("pipeline_id", pipelineId),
						zap.String("source", sourceName),
						zap.String("destination", destinationName))
					continue
				}
			}

			logger.GetLoggerWithContext(ctx).Info("checking pipeline flow for deviation",
				zap.String("tenant_id", tenantId),
				zap.String("pipeline_id", pipelineId),
				zap.String("source", sourceName),
				zap.String("destination", destinationName))

			// Build pipeline stages based on source and pipeline configuration
			stages, err := buildStageFlow(ctx, db, pipelineMapping)
			if err != nil {
				errorMsg := fmt.Sprintf("error building stage flow for pipeline %s in tenant %s: %v", pipelineId, tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLoggerWithContext(ctx).Error("Error building stage flow",
					zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
				continue
			}

			logger.GetLogger().Info("stages for pipeline " + pipelineMapping.Pipeline.Name + ": " + strings.Join(func() []string {
				var s []string
				for _, stage := range stages {
					s = append(s, stage.StageID)
				}
				return s
			}(), " -> "))

			if len(stages) == 2 {
				// Pipeline has only ingestion and dispenser, skip as no processing stages to monitor
				logger.GetLoggerWithContext(ctx).Info("Skipping pipeline with only ingestion and dispenser stages, passthrough pipelines are not monitored",
					zap.String("tenant_id", tenantId),
					zap.String("pipeline_id", pipelineId))
				continue
			}

			// Analyze each stage for deviations
			for i, stage := range stages {
				// Skip analysis for first stage (ingestion) as it has no input stage to compare
				if i == 0 {
					continue
				}

				// Get input events from previous stage (or ingestion for first processing stage)
				var inputEvents float64
				if i == 1 {
					// For first processing stage, get ingestion events
					inputEvents, err = getStageEventCount(ctx, t.Id, pipelineMapping, ComponentIngestion, "total_events_delivered", startTime, endTime)
					if err != nil {
						errorMsg := fmt.Sprintf("error getting ingestion events for pipeline %s in tenant %s: %v", pipelineId, tenantId, err)
						jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
						logger.GetLoggerWithContext(ctx).Error("Error getting ingestion events",
							zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
						continue
					}
				} else {
					// Use output from previous stage
					inputEvents = stages[i-1].OutputEvents
				}

				// Get output events for current stage
				outputEvents, err := getStageEventCount(ctx, t.Id, pipelineMapping, stage.ComponentName, "total_events_delivered", startTime, endTime)
				if err != nil {
					errorMsg := fmt.Sprintf("error getting stage output events for pipeline %s stage %s in tenant %s: %v", pipelineId, stage.StageID, tenantId, err)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLoggerWithContext(ctx).Error("Error getting stage output events",
						zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId), zap.String("stage", stage.StageID))
					continue
				}

				// Update stage with actual events
				stages[i].InputEvents = inputEvents
				stages[i].OutputEvents = outputEvents

				// Skip if insufficient events to warrant analysis
				if inputEvents < float64(minEventsThreshold) {
					logger.GetLoggerWithContext(ctx).Debug("Insufficient events for analysis",
						zap.String("tenant_id", tenantId),
						zap.String("pipeline_id", pipelineId),
						zap.String("stage", stage.StageID),
						zap.Float64("input_events", inputEvents))
					continue
				}

				// Calculate deviation percentage
				var deviationPct float64
				if inputEvents > 0 {
					deviationPct = math.Abs(((inputEvents - outputEvents) / inputEvents) * 100)
				}

				logger.GetLoggerWithContext(ctx).Debug("Stage flow analysis",
					zap.String("tenant_id", tenantId),
					zap.String("pipeline_id", pipelineId),
					zap.String("stage", stage.StageID),
					zap.Float64("input_events", inputEvents),
					zap.Float64("output_events", outputEvents),
					zap.Float64("deviation_pct", deviationPct))

				// Check if deviation exceeds threshold for monitored services
				shouldAlert := shouldAlertForStage(stage, deviationPct, deviationThreshold)

				if shouldAlert {
					logger.GetLoggerWithContext(ctx).Info("Significant deviation detected in pipeline stage, sending alert",
						zap.String("tenant_id", tenantId),
						zap.String("pipeline_id", pipelineId),
						zap.String("stage", stage.StageID),
						zap.Float64("deviation_pct", deviationPct))

					// Get source data plane ID
					sourceDataPlaneID, err := getPipelineSourceDataPlaneID(ctx, db, pipelineMapping.LogSourceID)
					if err != nil {
						errorMsg := fmt.Sprintf("error getting source data plane ID for source %s in tenant %s: %v", pipelineMapping.LogSourceID.String(), tenantId, err)
						jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
						logger.GetLoggerWithContext(ctx).Error("Error getting source data plane ID",
							zap.Error(err), zap.String("tenant_id", tenantId), zap.String("source_id", pipelineMapping.LogSourceID.String()))
						continue
					}

					flowAlert := model.NewPipelineFlowDeviationAlert(&t, &pipelineMapping, sourceDataPlaneID,
						stages[i], startTime, endTime)

					alert, err := buildPipelineFlowDeviationAlert(*flowAlert, deviationThreshold)
					if err != nil {
						errorMsg := fmt.Sprintf("error building pipeline flow deviation alert for pipeline %s in tenant %s: %v", pipelineId, tenantId, err)
						jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
						logger.GetLoggerWithContext(ctx).Error("Error building pipeline flow deviation alert",
							zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
						continue
					}

					err = alertsManager.SendAlerts([]*alerts_async.Alert{alert})
					if err != nil {
						errorMsg := fmt.Sprintf("error sending pipeline flow deviation alert for pipeline %s in tenant %s: %v", pipelineId, tenantId, err)
						jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
						logger.GetLoggerWithContext(ctx).Error("Error sending pipeline flow deviation alert",
							zap.Error(err), zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId))
						continue
					}

					logger.GetLoggerWithContext(ctx).Info("Successfully sent pipeline flow deviation alert",
						zap.String("tenant_id", tenantId), zap.String("pipeline_id", pipelineId), zap.String("stage", stage.StageID))
				} else {
					// Stage is healthy, add to healthy stages for auto-resolution
					healthyStages = append(healthyStages, pipelineStageKey{
						pipelineId: pipelineId,
						sourceId:   pipelineMapping.LogSourceID.String(),
						stageId:    stage.StageID,
					})
				}
			}
		}

		// Auto-resolve any open pipeline flow deviation alerts for healthy stages
		if len(healthyStages) > 0 {
			var alertsToDismiss []string
			for _, stageKey := range healthyStages {
				q := fmt.Sprintf("tenantId:%s AND dismissed:false AND functionalityType:%s AND functionalityEntityId:%s AND secondaryEntityId:%s",
					tenantId, alerts_async.VolumeDeviationChecker.String(), stageKey.sourceId, stageKey.stageId)
				openAlerts, _, err := os.Search(ctx, osClient, common.AlertsIndex, q)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error while searching pipeline flow alerts to auto-resolve", zap.Error(err), zap.String("query", q))
					continue
				}
				alerts, err := statistics.ParseAlertDocuments(openAlerts)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error while decoding OpenSearch pipeline flow alert response", zap.Error(err), zap.String("tenantId", tenantId))
					continue
				}
				for _, alrt := range alerts {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
			if len(alertsToDismiss) > 0 {
				if err := alertsManager.AutoResolveAlerts(alertsToDismiss); err != nil {
					logger.GetLoggerWithContext(ctx).Error("error while auto-resolving pipeline flow alerts", zap.Error(err), zap.String("tenant_id", tenantId))
				} else {
					logger.GetLoggerWithContext(ctx).Info("auto-resolved pipeline flow alerts for healthy stages", zap.String("tenant_id", tenantId), zap.Any("alertsToDismiss", alertsToDismiss))
				}
			}
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLoggerWithContext(ctx).Info("successfully completed pipeline flow deviation alert processing")
		return common.NewJobResultSuccess()
	} else {
		logger.GetLoggerWithContext(ctx).Info("pipeline flow deviation alert processing completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}

// buildStageFlow builds the pipeline stages based on source and pipeline configuration
func buildStageFlow(ctx context.Context, db *gorm.DB, pipelineMapping pipeline.PipelineWithMappings) ([]model.PipelineStage, error) {
	var stages []model.PipelineStage
	// 1. Initial Ingestion Stage (based on source type)
	stages = append(stages, model.PipelineStage{
		StageID:       "INPUT",
		ComponentName: "ingestion",
	})

	// 3. VC and Enrichment Stage Logic
	hasVC, _, err := pipeline.HasActiveRules(ctx, db, pipelineMapping.Pipeline.ID, pipelineMapping.Pipeline.TenantID)
	if err != nil {
		return nil, fmt.Errorf("error checking active rules: %w", err)
	}

	// Check for active enrichment configurations
	hasEnrichment, _, err := enrichment.HasActiveEnrichment(ctx, db, pipelineMapping.Pipeline.ID, pipelineMapping.Pipeline.TenantID)
	if err != nil {
		return nil, fmt.Errorf("error checking active enrichment: %w", err)
	}

	// Check for active transformation configurations
	hasTransformation, _, err := data_transformation.HasActiveTransformation(ctx, db, pipelineMapping.Pipeline.ID, pipelineMapping.Pipeline.TenantID)
	if err != nil {
		return nil, fmt.Errorf("error checking active transformation: %w", err)
	}

	// only add normalization stage if VC, enrichment, or transformation is present
	if hasVC || hasEnrichment || hasTransformation {
		stages = append(stages, model.PipelineStage{
			StageID:       "NORMALIZATION",
			ComponentName: ComponentParsing,
		})

	}

	if hasVC && hasEnrichment {
		isEnrichmentBefore, err := isEnrichmentBeforeVC(ctx, db, pipelineMapping.Pipeline.ID, pipelineMapping.Pipeline.TenantID)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Warn("Error checking enrichment order, defaulting to VC first", zap.Error(err))
			isEnrichmentBefore = false
		}

		if isEnrichmentBefore {
			stages = append(stages, model.PipelineStage{
				StageID:       "ENRICHMENT",
				ComponentName: ComponentEnrichment,
			})
			stages = append(stages, model.PipelineStage{
				StageID:       "VC_RULE",
				ComponentName: ComponentRuleEngine,
			})
		} else {
			stages = append(stages, model.PipelineStage{
				StageID:       "VC_RULE",
				ComponentName: ComponentRuleEngine,
			})
			stages = append(stages, model.PipelineStage{
				StageID:       "ENRICHMENT",
				ComponentName: ComponentEnrichment,
			})
		}
	} else if hasVC {
		stages = append(stages, model.PipelineStage{
			StageID:       "VC_RULE",
			ComponentName: ComponentRuleEngine,
		})
	} else if hasEnrichment {
		stages = append(stages, model.PipelineStage{
			StageID:       "ENRICHMENT",
			ComponentName: ComponentEnrichment,
		})
	}

	// Add transformation stage only if there are active transformations
	if hasTransformation {
		stages = append(stages, model.PipelineStage{
			StageID:       "TRANSFORMATION",
			ComponentName: ComponentTransformation,
		})
	}

	stages = append(stages, model.PipelineStage{
		StageID:       "DESTINATION",
		ComponentName: ComponentDispenser,
	})

	return stages, nil
}

// isEnrichmentBeforeVC determines if enrichment should come before volume control
func isEnrichmentBeforeVC(ctx context.Context, db *gorm.DB, pipelineID, tenantID uuid.UUID) (bool, error) {
	rules, err := vc_rule.GetActiveVCRulesByPipelineAndTenant(pipelineID, tenantID, db)
	if err != nil {
		return false, err
	}

	for _, rule := range rules {
		// Check if rule references attributes starting with "db_enriched_"
		referencedAttrs, err := rule.GetReferencedAttributes()
		if err != nil {
			logger.GetLoggerWithContext(ctx).Warn("Error getting referenced attributes for rule",
				zap.String("rule_id", rule.ID.String()), zap.Error(err))
			continue
		}

		for _, attr := range referencedAttrs {
			if strings.HasPrefix(attr, "db_enriched_") {
				return true, nil
			}
		}
	}

	return false, nil
}

// getStageEventCount gets the event count for a specific stage
func getStageEventCount(ctx context.Context, tenantId uuid.UUID, pipelineMapping pipeline.PipelineWithMappings, componentName, metricName string, startTime, endTime time.Time) (float64, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)

	// Build query based on component type
	var query string
	switch componentName {
	case ComponentIngestion:
		query = fmt.Sprintf("tags.component_name: \"%s\" AND name: \"%s\" AND tags.db_event_source_id: \"%s\"",
			componentName, metricName, pipelineMapping.LogSourceID.String())
	case ComponentDispenser:
		// For dispenser, we need to include destination_id
		query = fmt.Sprintf("tags.component_name: \"%s\" AND name: \"%s\" AND tags.db_event_source_id: \"%s\" AND tags.destination_id: \"%s\"",
			componentName, metricName, pipelineMapping.LogSourceID.String(), pipelineMapping.DestinationID.String())
	case ComponentParsing, ComponentEnrichment, ComponentRuleEngine, ComponentTransformation:
		query = fmt.Sprintf("tags.component_name: \"%s\" AND name: \"%s\" AND tags.db_event_source_id: \"%s\" AND tags.db_pipeline_id: \"%s\"",
			componentName, metricName, pipelineMapping.LogSourceID.String(), pipelineMapping.Pipeline.ID.String())
	default:
		// return error
		return 0, fmt.Errorf("unsupported component name for event count: %s", componentName)
	}

	response, err := statistics.GetStatsSum(ctx, query, tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, fmt.Errorf("error getting stats for component %s: %w", componentName, err)
	}

	return response.Sum, nil
}

// getPipelineSourceDataPlaneID gets the data plane ID for a source
func getPipelineSourceDataPlaneID(ctx context.Context, db *gorm.DB, sourceId uuid.UUID) (uuid.UUID, error) {
	var src source.Source
	err := db.WithContext(ctx).Where("id = ?", sourceId).First(&src).Error
	if err != nil {
		return uuid.Nil, fmt.Errorf("error getting source data plane ID: %w", err)
	}
	return src.DataPlaneId, nil
}

// shouldAlertForStage determines if an alert should be sent for a specific stage
func shouldAlertForStage(stage model.PipelineStage, deviationPct, threshold float64) bool {
	// Only alert for monitored services
	monitoredServices := []string{
		ComponentParsing,
		ComponentTransformation,
		ComponentEnrichment,
	}

	isMonitored := false
	for _, service := range monitoredServices {
		if stage.ComponentName == service {
			isMonitored = true
			break
		}
	}

	if !isMonitored {
		return false
	}

	return deviationPct >= threshold
}

// buildPipelineFlowDeviationAlert builds an alert for pipeline flow deviation
func buildPipelineFlowDeviationAlert(flowAlert model.PipelineFlowDeviationAlert, deviationThreshold float64) (*alerts_async.Alert, error) {
	pipelineName := flowAlert.Pipeline.Pipeline.Name
	stageID := flowAlert.DeviationStage.StageID

	// Calculate deviation percentage locally
	var deviationPct float64
	if flowAlert.DeviationStage.InputEvents > 0 {
		deviationPct = ((flowAlert.DeviationStage.InputEvents - flowAlert.DeviationStage.OutputEvents) / flowAlert.DeviationStage.InputEvents) * 100
		if deviationPct < 0 {
			deviationPct = -deviationPct // Make it absolute
		}
	}

	title := fmt.Sprintf("Significant Event Deviation (%.1f%%) in %s for Pipeline '%s'",
		deviationPct, stageID, pipelineName)

	message := fmt.Sprintf(
		"Significant event flow deviation detected in pipeline '%s' at stage '%s'. "+
			"As of '%s', between '%s' and '%s', %.1f events were input to the stage and %.1f events were output "+
			"(%.1f%% deviation, threshold: %.1f%%). This may indicate processing issues in %s.",
		pipelineName,
		stageID,
		util.HumanReadableTimeWithZone(time.Now()),
		util.HumanReadableTimeWithZone(flowAlert.CheckStartTime),
		util.HumanReadableTimeWithZone(flowAlert.CheckEndTime),
		flowAlert.DeviationStage.InputEvents,
		flowAlert.DeviationStage.OutputEvents,
		deviationPct,
		deviationThreshold,
		stageID,
	)

	return alerts_async.NewAlert(
		alerts_async.LogSource,
		alerts_async.WithEntity(flowAlert),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.VolumeDeviationChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithAlertType(alerts_async.Internal),
		alerts_async.WithErrorCode(alerts_async.DNDW10001, "Pipeline stage event flow deviation"),
		alerts_async.WithAction("Please review pipeline stages configuration and statistics from graph view at source page."),
	)
}
