package vc

import (
	"context"
	"errors"
	"fmt"

	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/vc_rule"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
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
			zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()))
		return alerts, healthyRuleIds, nil
	}

	logger.GetLogger().Debug("Processing unmatched no route processor alerts",
		zap.String("tenantId", data.Tenant.Id.String()),
		zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()))

	unmatchedAlerts, healthyRules, err := p.checkUnmatchedNoRouteProcessorConditions(ctx, data)
	if err != nil {
		return nil, nil, fmt.Errorf("error checking unmatched no route processor alerts: %w", err)
	}

	for _, vcAlert := range unmatchedAlerts {
		alert, err := p.BuildAlert(vcAlert, data.Config)
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

// AutoResolveAlerts auto-resolves unmatched no route processor alerts for healthy rules
func (p *UnmatchedNoRouteProcessor) AutoResolveAlerts(ctx context.Context, data *ProcessingData, healthyRuleIds []string) error {
	if len(healthyRuleIds) == 0 {
		return nil
	}

	return autoResolveAlertsHelper(ctx, data, healthyRuleIds, alerts_async.VcUnmatchedNoRouteProcessor, "UnmatchedNoRouteProcessor")
}

// checkUnmatchedNoRouteProcessorConditions implements the complex Alert 3 logic
func (p *UnmatchedNoRouteProcessor) checkUnmatchedNoRouteProcessorConditions(ctx context.Context, data *ProcessingData) ([]*UnmatchedNoRouteProcessorAlert, []uuid.UUID, error) {
	var vcAlerts []*UnmatchedNoRouteProcessorAlert
	var healthyRules []uuid.UUID

	// Track rules we've checked to avoid duplicates in healthy list
	checkedRules := make(map[uuid.UUID]bool)
	sourceId := data.PipelineMapping.LogSourceID

	// Check 1: sendUnmatchedEvents to primary destination
	sendUnmatchedToPrimary, err := p.getSourceConfiguration(ctx, sourceId)
	if err != nil {
		logger.GetLogger().Error("error getting source configuration",
			zap.Error(err), zap.String("sourceId", sourceId.String()))
		return nil, nil, err
	}

	// Check 2: Route processor configuration
	hasValidRouteProcessor, err := p.checkRouteProcessorConfiguration(ctx, data.PipelineMapping.Pipeline.ID, sourceId)
	if err != nil {
		logger.GetLogger().Error("error checking route processor configuration",
			zap.Error(err), zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()), zap.String("sourceId", sourceId.String()))
		return nil, nil, err
	}

	// Check 3: Get rule with lowest priority and check drop percentage
	lowestPriorityRule, err := p.getRuleWithLowestPriority(ctx, data.PipelineMapping.Pipeline.ID, data.Tenant.Id)
	if err != nil {
		logger.GetLogger().Error("error getting lowest priority rule",
			zap.Error(err), zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()))
		return nil, nil, err
	}

	if lowestPriorityRule == nil {
		logger.GetLogger().Debug("no rules found for pipeline",
			zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()))
		return vcAlerts, healthyRules, nil
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
		return vcAlerts, healthyRules, nil
	}

	// If pipeline has valid route processor configuration, rule is healthy
	if hasValidRouteProcessor {
		if !checkedRules[lowestPriorityRule.ID] {
			healthyRules = append(healthyRules, lowestPriorityRule.ID)
			checkedRules[lowestPriorityRule.ID] = true
			logger.GetLogger().Debug("rule is healthy due to valid route processor",
				zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()),
				zap.String("sourceId", sourceId.String()),
				zap.String("ruleId", lowestPriorityRule.ID.String()))
		}
		return vcAlerts, healthyRules, nil
	}

	// Check if the rule's drop percentage exceeds threshold
	exceedsThreshold, todayEvaluated, todayMatched, err := p.checkRuleDropPercentage(lowestPriorityRule, sourceId, data)
	if err != nil {
		logger.GetLogger().Error("error checking rule drop percentage",
			zap.Error(err), zap.String("ruleId", lowestPriorityRule.ID.String()))
		return nil, nil, fmt.Errorf("error checking rule drop percentage: %w", err)
	}

	if exceedsThreshold {
		// Create alert for this combination
		var matchedPercent, unmatchedPercent float64
		if todayEvaluated > 0 {
			matchedPercent = (float64(todayMatched) / float64(todayEvaluated)) * 100
			unmatchedPercent = 100 - matchedPercent
		}

		vcAlert := NewUnmatchedNoRouteProcessorAlert(data.Tenant, &data.PipelineMapping.Pipeline, lowestPriorityRule.ID, lowestPriorityRule.Name, sourceId,
			todayEvaluated, todayMatched, matchedPercent, unmatchedPercent)

		vcAlerts = append(vcAlerts, vcAlert)

		logger.GetLogger().Info("Created unmatched no route processor alert",
			zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()),
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

	return vcAlerts, healthyRules, nil
}

// Helper methods for UnmatchedNoRouteProcessor
func (p *UnmatchedNoRouteProcessor) getSourceConfiguration(ctx context.Context, sourceId uuid.UUID) (sendUnmatchedToPrimary bool, err error) {
	// Get the source from cached sources if available, otherwise query database
	// For now, query the database directly
	var source source.Source
	err = p.db.WithContext(ctx).Where("id = ?", sourceId).First(&source).Error
	if err != nil {
		return false, fmt.Errorf("error getting source: %w", err)
	}

	// Parse the advanced configuration
	advancedConfig, err := source.GetAdvancedConfiguration()
	if err != nil {
		return false, fmt.Errorf("error parsing source advanced_configuration: %w", err)
	}

	return advancedConfig.SendUnmatchedEventToPrimaryDestination, nil
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

func (p *UnmatchedNoRouteProcessor) getRuleWithLowestPriority(ctx context.Context, pipelineId, tenantId uuid.UUID) (*vc_rule.VCRule, error) {
	rule, err := vc_rule.GetLowestPriorityActiveRule(pipelineId, tenantId, p.db)
	if err != nil {
		return nil, fmt.Errorf("error getting rule with lowest priority: %w", err)
	}
	return rule, nil
}

func (p *UnmatchedNoRouteProcessor) checkRuleDropPercentage(vcRule *vc_rule.VCRule, sourceId uuid.UUID, data *ProcessingData) (bool, int64, int64, error) {
	if vcRule == nil {
		return false, 0, 0, nil
	}

	ingestedCount, err := data.StatsService.GetPipelineIngestionStats(data.PipelineMapping.Pipeline.ID.String(), sourceId.String(), data.Config.TodayStart, data.Config.TodayEnd)
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

// BuildAlert builds an alert specifically for unmatched no route processor cases
func (p *UnmatchedNoRouteProcessor) BuildAlert(vcAlert *UnmatchedNoRouteProcessorAlert, config *VCAlertConfig) (*alerts_async.Alert, error) {
	title := fmt.Sprintf("Volume Controller: '%s' has high unmatched events with no route processor", vcAlert.RuleName)
	message := fmt.Sprintf("Rule '%s' in pipeline '%s' has %.2f%% unmatched events (%s unmatched out of %s evaluated), "+
		"exceeding the %.1f%% threshold. The source is not configured to send unmatched events to primary destination "+
		"and the pipeline lacks proper route processor configuration for handling unmatched events. "+
		"These events may be lost or not processed properly.",
		vcAlert.RuleName, vcAlert.Pipeline.Name, vcAlert.UnmatchedPercent,
		util.HumanReadableNumber(vcAlert.TodayEvaluated-vcAlert.TodayMatched), util.HumanReadableNumber(vcAlert.TodayEvaluated), config.UnmatchedNoRouteProcessorThreshold)

	return alerts_async.NewAlert(
		alerts_async.VolumeControlRule,
		alerts_async.WithEntity(vcAlert),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.VcUnmatchedNoRouteProcessor),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10006, "Volume control rule condition detected"),
	)
}
