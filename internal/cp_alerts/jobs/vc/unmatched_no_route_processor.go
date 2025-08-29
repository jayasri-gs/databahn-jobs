package vc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/db-models/rule"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// UnmatchedNoRouteProcessor handles unmatched no route processor alerts
type UnmatchedNoRouteProcessor struct {
	db *gorm.DB
}

func (p *UnmatchedNoRouteProcessor) GetAlertType() string {
	return "UnmatchedNoRouteProcessor"
}

func (p *UnmatchedNoRouteProcessor) ProcessAlerts(ctx context.Context, data *ProcessingData) ([]*alerts_async.Alert, []string, error) {
	var alerts []*alerts_async.Alert
	var healthyRuleIds []string

	// Only process if pipeline has rules
	if len(data.VCRules) == 0 {
		logger.GetLogger().Debug("skipping unmatched no route processor check - no rules",
			zap.String("pipelineId", data.Pipeline.ID.String()))
		return alerts, healthyRuleIds, nil
	}

	logger.GetLogger().Debug("Processing unmatched no route processor alerts",
		zap.String("tenantId", data.Tenant.Id.String()),
		zap.String("pipelineId", data.Pipeline.ID.String()))

	unmatchedAlerts, healthyRules, err := p.checkUnmatchedNoRouteProcessorConditions(ctx, data)
	if err != nil {
		return nil, nil, fmt.Errorf("error checking unmatched no route processor alerts: %w", err)
	}

	for _, vcAlert := range unmatchedAlerts {
		alert, err := buildVCAlert(*vcAlert, data.Config)
		if err != nil {
			logger.GetLogger().Error("error building unmatched no route processor alert", zap.Error(err),
				zap.String("tenantId", data.Tenant.Id.String()), zap.String("ruleId", vcAlert.RuleID.String()))
			continue
		}
		alerts = append(alerts, alert)

		logger.GetLogger().Info("Unmatched no route processor alert created",
			zap.String("tenantId", data.Tenant.Id.String()),
			zap.String("ruleId", vcAlert.RuleID.String()))
	}

	// Add healthy rules to auto-resolution list
	for _, ruleId := range healthyRules {
		healthyRuleIds = append(healthyRuleIds, ruleId.String())
		logger.GetLogger().Info("Rule is healthy, adding to auto-resolution list",
			zap.String("tenantId", data.Tenant.Id.String()),
			zap.String("ruleId", ruleId.String()))
	}

	return alerts, healthyRuleIds, nil
}

// checkUnmatchedNoRouteProcessorConditions implements the complex Alert 3 logic
func (p *UnmatchedNoRouteProcessor) checkUnmatchedNoRouteProcessorConditions(ctx context.Context, data *ProcessingData) ([]*model.VCAlert, []uuid.UUID, error) {
	var vcAlerts []*model.VCAlert
	var healthyRules []uuid.UUID

	// Track rules we've checked to avoid duplicates in healthy list
	checkedRules := make(map[uuid.UUID]bool)

	for _, logSource := range data.LogSources {
		sourceId := logSource.LogSourceID

		// Check 1: sendUnmatchedEvents to primary destination
		sendUnmatchedToPrimary, err := p.getSourceConfiguration(ctx, sourceId)
		if err != nil {
			logger.GetLogger().Error("error getting source configuration",
				zap.Error(err), zap.String("sourceId", sourceId.String()))
			continue
		}

		// Check 2: Route processor configuration
		hasValidRouteProcessor, err := p.checkRouteProcessorConfiguration(ctx, data.Pipeline.ID, sourceId)
		if err != nil {
			logger.GetLogger().Error("error checking route processor configuration",
				zap.Error(err), zap.String("pipelineId", data.Pipeline.ID.String()), zap.String("sourceId", sourceId.String()))
			continue
		}

		// Check 3: Get rule with lowest priority and check drop percentage
		lowestPriorityRule, err := p.getRuleWithLowestPriority(ctx, data.Pipeline.ID, data.Tenant.Id)
		if err != nil {
			logger.GetLogger().Error("error getting lowest priority rule",
				zap.Error(err), zap.String("pipelineId", data.Pipeline.ID.String()))
			continue
		}

		if lowestPriorityRule == nil {
			logger.GetLogger().Debug("no rules found for pipeline",
				zap.String("pipelineId", data.Pipeline.ID.String()))
			continue
		}

		// If sendUnmatchedEvents to primary destination is true, rule is healthy
		if sendUnmatchedToPrimary {
			if !checkedRules[lowestPriorityRule.ID] {
				healthyRules = append(healthyRules, lowestPriorityRule.ID)
				checkedRules[lowestPriorityRule.ID] = true
				logger.GetLogger().Debug("rule is healthy due to sendUnmatchedEvents enabled",
					zap.String("sourceId", sourceId.String()),
					zap.String("ruleId", lowestPriorityRule.ID.String()))
			}
			continue
		}

		// If pipeline has valid route processor configuration, rule is healthy
		if hasValidRouteProcessor {
			if !checkedRules[lowestPriorityRule.ID] {
				healthyRules = append(healthyRules, lowestPriorityRule.ID)
				checkedRules[lowestPriorityRule.ID] = true
				logger.GetLogger().Debug("rule is healthy due to valid route processor",
					zap.String("pipelineId", data.Pipeline.ID.String()),
					zap.String("sourceId", sourceId.String()),
					zap.String("ruleId", lowestPriorityRule.ID.String()))
			}
			continue
		}

		// Check if the rule's drop percentage exceeds threshold
		exceedsThreshold, todayEvaluated, todayMatched, err := p.checkRuleDropPercentage(lowestPriorityRule, sourceId, data)
		if err != nil {
			logger.GetLogger().Error("error checking rule drop percentage",
				zap.Error(err), zap.String("ruleId", lowestPriorityRule.ID.String()))
			continue
		}

		if exceedsThreshold {
			// Create alert for this combination
			var matchedPercent, unmatchedPercent float64
			if todayEvaluated > 0 {
				matchedPercent = (float64(todayMatched) / float64(todayEvaluated)) * 100
				unmatchedPercent = 100 - matchedPercent
			}

			vcAlert := model.NewVCAlert(data.Tenant, data.Pipeline, lowestPriorityRule.ID, lowestPriorityRule.Name, sourceId,
				model.VCAlertTypeUnmatchedNoRouteProcessor,
				todayMatched, 0, todayEvaluated, 0, // No yesterday data needed for this alert
				matchedPercent, unmatchedPercent)

			vcAlerts = append(vcAlerts, vcAlert)

			logger.GetLogger().Info("Created unmatched no route processor alert",
				zap.String("pipelineId", data.Pipeline.ID.String()),
				zap.String("sourceId", sourceId.String()),
				zap.String("ruleId", lowestPriorityRule.ID.String()),
				zap.Float64("unmatchedPercent", unmatchedPercent))
		} else {
			// Rule doesn't exceed threshold and has sufficient traffic, it's healthy
			if todayEvaluated >= data.Config.MinimumEventsThreshold && !checkedRules[lowestPriorityRule.ID] {
				healthyRules = append(healthyRules, lowestPriorityRule.ID)
				checkedRules[lowestPriorityRule.ID] = true
				logger.GetLogger().Debug("rule is healthy due to low drop percentage with sufficient traffic",
					zap.String("ruleId", lowestPriorityRule.ID.String()),
					zap.Int64("todayEvaluated", todayEvaluated),
					zap.Int64("minimumThreshold", data.Config.MinimumEventsThreshold))
			}
		}
	}

	return vcAlerts, healthyRules, nil
}

// Helper methods for UnmatchedNoRouteProcessor
func (p *UnmatchedNoRouteProcessor) getSourceConfiguration(ctx context.Context, sourceId uuid.UUID) (sendUnmatchedToPrimary bool, err error) {
	// Define a struct to hold only the advanced_configuration field
	var source struct {
		AdvancedConfiguration json.RawMessage `gorm:"column:advanced_configuration"`
	}

	err = p.db.WithContext(ctx).Table("log_source").
		Select("advanced_configuration").
		Where("id = ?", sourceId).
		Take(&source).Error

	if err != nil {
		return false, fmt.Errorf("error getting source advanced_configuration: %w", err)
	}

	// Parse the advanced_configuration JSON to check sendUnmatchedEventToPrimaryDestination
	if len(source.AdvancedConfiguration) > 0 {
		var advancedConfig map[string]interface{}
		err = json.Unmarshal(source.AdvancedConfiguration, &advancedConfig)
		if err != nil {
			return false, fmt.Errorf("error parsing source advanced_configuration: %w", err)
		}

		// Check for sendUnmatchedEventToPrimaryDestination
		if sendUnmatched, exists := advancedConfig["sendUnmatchedEventToPrimaryDestination"]; exists {
			if sendUnmatchedBool, ok := sendUnmatched.(bool); ok {
				return sendUnmatchedBool, nil
			}
		}
	}

	// Default to false if not specified
	return false, nil
}

func (p *UnmatchedNoRouteProcessor) checkRouteProcessorConfiguration(ctx context.Context, pipelineId, sourceId uuid.UUID) (hasValidRouteProcessor bool, err error) {
	var routeProcessor struct {
		UnmatchedAction        string `json:"unmatched_action"`
		ExplicitDropAction     string `json:"explicit_drop_action"`
		SecondaryDestinationId string `json:"secondary_destination_id"`
	}

	// Query route processor from change flag
	err = p.db.WithContext(ctx).Table("route_processor").
		Where("pipeline_id = ? AND has_vc_route_processor = true",
			pipelineId.String()).
		First(&routeProcessor).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("error checking route processor: %w", err)
	}

	if routeProcessor.UnmatchedAction == "PUBLISH" || routeProcessor.ExplicitDropAction == "PUBLISH" {
		return true, nil
	}

	return false, nil
}

func (p *UnmatchedNoRouteProcessor) getRuleWithLowestPriority(ctx context.Context, pipelineId, tenantId uuid.UUID) (*rule.Rule, error) {
	var vcRuleResult VCRuleQueryResult
	err := p.db.WithContext(ctx).Table("vc_rule").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
					pipelineId.String(), tenantId.String(), VCRuleStatusActive).
		Order("priority DESC"). // Higher priority number = lower priority
		First(&vcRuleResult).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil // No rules found
		}
		return nil, fmt.Errorf("error getting rule with lowest priority: %w", err)
	}

	return &vcRuleResult.Rule, nil
}

func (p *UnmatchedNoRouteProcessor) checkRuleDropPercentage(vcRule *rule.Rule, sourceId uuid.UUID, data *ProcessingData) (bool, int64, int64, error) {
	if vcRule == nil {
		return false, 0, 0, nil
	}

	ingestedCount, err := data.StatsService.GetPipelineIngestionStats(data.Pipeline.ID.String(), sourceId.String(), data.Config.TodayStart, data.Config.TodayEnd)
	if err != nil {
		return false, 0, 0, fmt.Errorf("error getting pipeline ingestion stats: %w", err)
	}

	matchedCount, err := data.StatsService.GetRuleMatchedStats(sourceId.String(), vcRule.ID.String(), data.Config.TodayStart, data.Config.TodayEnd)
	if err != nil {
		return false, 0, 0, fmt.Errorf("error getting rule matched stats: %w", err)
	}

	// Only check if there's sufficient traffic
	if ingestedCount < data.Config.MinimumEventsThreshold {
		return false, ingestedCount, matchedCount, nil
	}

	// Calculate drop percentage (unmatched percentage)
	var dropPercent float64
	if ingestedCount > 0 {
		todayUnmatched := ingestedCount - matchedCount
		dropPercent = (float64(todayUnmatched) / float64(ingestedCount)) * 100
	}

	// Check if drop percentage exceeds threshold
	if dropPercent > data.Config.UnmatchedNoRouteProcessorThreshold {
		logger.GetLogger().Info("Rule drop percentage exceeds threshold",
			zap.String("ruleId", vcRule.ID.String()),
			zap.String("ruleName", vcRule.Name),
			zap.Float64("dropPercent", dropPercent),
			zap.Float64("threshold", data.Config.UnmatchedNoRouteProcessorThreshold),
			zap.Int64("todayIngested", ingestedCount),
			zap.Int64("todayMatched", matchedCount))
		return true, ingestedCount, matchedCount, nil
	}

	return false, ingestedCount, matchedCount, nil
}
