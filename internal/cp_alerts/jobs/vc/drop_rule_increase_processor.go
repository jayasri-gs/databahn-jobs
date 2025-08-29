package vc

import (
	"context"
	"fmt"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/db-models/rule"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

// DropRuleIncreaseProcessor handles DROP rule increase alerts
type DropRuleIncreaseProcessor struct{}

func (p *DropRuleIncreaseProcessor) GetAlertType() string {
	return "DropRuleIncrease"
}

func (p *DropRuleIncreaseProcessor) ProcessAlerts(_ context.Context, data *ProcessingData) ([]*alerts_async.Alert, []string, error) {
	var alerts []*alerts_async.Alert
	var healthyRuleIds []string

	logger.GetLogger().Debug("Processing DROP rule increase alerts",
		zap.String("tenantId", data.Tenant.Id.String()),
		zap.String("pipelineId", data.Pipeline.ID.String()),
		zap.Int("ruleCount", len(data.VCRules)))

	for _, vcRule := range data.VCRules {
		for _, logSource := range data.LogSources {
			sourceId := logSource.LogSourceID

			// Get today's and yesterday's rule statistics
			todayStats, err := data.StatsService.GetRuleStats(sourceId.String(), vcRule.ID.String(), data.Config.TodayStart, data.Config.TodayEnd)
			if err != nil {
				logger.GetLogger().Error("error getting today's rule stats", zap.Error(err),
					zap.String("tenantId", data.Tenant.Id.String()), zap.String("ruleId", vcRule.ID.String()))
				continue
			}

			yesterdayStats, err := data.StatsService.GetRuleStats(sourceId.String(), vcRule.ID.String(), data.Config.YesterdayStart, data.Config.YesterdayEnd)
			if err != nil {
				logger.GetLogger().Error("error getting yesterday's rule stats", zap.Error(err),
					zap.String("tenantId", data.Tenant.Id.String()), zap.String("ruleId", vcRule.ID.String()))
				continue
			}

			logger.GetLogger().Debug("rule statistics",
				zap.String("ruleId", vcRule.ID.String()),
				zap.String("ruleName", vcRule.Name),
				zap.String("actionType", vcRule.ActionType),
				zap.Int64("todayEvaluated", todayStats.Evaluated),
				zap.Int64("todayMatched", todayStats.Matched),
				zap.Int64("yesterdayEvaluated", yesterdayStats.Evaluated),
				zap.Int64("yesterdayMatched", yesterdayStats.Matched))

			// Check alert conditions
			shouldAlert := p.checkDropRuleIncreaseCondition(vcRule, todayStats, yesterdayStats, data.Config)

			if shouldAlert {
				// Calculate percentages
				var matchedPercent, unmatchedPercent float64
				if todayStats.Evaluated > 0 {
					matchedPercent = (float64(todayStats.Matched) / float64(todayStats.Evaluated)) * 100
					unmatchedPercent = 100 - matchedPercent
				}

				vcAlert := NewDropRuleIncreaseAlert(data.Tenant, data.Pipeline, vcRule.ID, vcRule.Name, sourceId,
					todayStats.Matched, yesterdayStats.Matched, todayStats.Evaluated, yesterdayStats.Evaluated,
					matchedPercent, unmatchedPercent)

				a, err := p.BuildAlert(vcAlert, data.Config)
				if err != nil {
					logger.GetLogger().Error("error building DROP rule increase alert", zap.Error(err),
						zap.String("tenantId", data.Tenant.Id.String()), zap.String("ruleId", vcRule.ID.String()))
					continue
				}

				alerts = append(alerts, a)

				logger.GetLogger().Info("DROP rule increase alert created",
					zap.String("tenantId", data.Tenant.Id.String()),
					zap.String("ruleId", vcRule.ID.String()))
			} else {
				// Only auto-resolve if rule has sufficient traffic
				if todayStats.Evaluated >= data.Config.MinimumEventsThreshold {
					healthyRuleIds = append(healthyRuleIds, vcRule.ID.String())
					logger.GetLogger().Info("Rule is healthy with sufficient traffic, adding to auto-resolution list",
						zap.String("tenantId", data.Tenant.Id.String()),
						zap.String("ruleId", vcRule.ID.String()),
						zap.String("ruleName", vcRule.Name),
						zap.Int64("todayEvaluated", todayStats.Evaluated),
						zap.Int64("minimumThreshold", data.Config.MinimumEventsThreshold))
				}
			}
		}
	}

	return alerts, healthyRuleIds, nil
}

// checkDropRuleIncreaseCondition checks if DROP rule increase condition is met
func (p *DropRuleIncreaseProcessor) checkDropRuleIncreaseCondition(vcRule rule.Rule, todayStats, yesterdayStats *RuleStats, config *VCAlertConfig) bool {
	// Only alert if there's significant traffic to avoid noise
	if todayStats.Evaluated < config.MinimumEventsThreshold {
		logger.GetLogger().Debug("skipping alert due to low traffic",
			zap.String("ruleId", vcRule.ID.String()),
			zap.Int64("todayEvaluated", todayStats.Evaluated),
			zap.Int64("minimumThreshold", config.MinimumEventsThreshold))
		return false
	}

	// Only for DROP rules
	if strings.ToUpper(vcRule.ActionType) != VCRuleActionTypeDrop {
		return false
	}

	// Need valid data for both days
	if yesterdayStats.Matched <= 0 || todayStats.Matched <= 0 || yesterdayStats.Evaluated <= 0 || todayStats.Evaluated <= 0 {
		return false
	}

	// Calculate percentages
	yesterdayMatchPercent := float64(yesterdayStats.Matched) / float64(yesterdayStats.Evaluated) * 100
	todayMatchPercent := float64(todayStats.Matched) / float64(todayStats.Evaluated) * 100

	// Check if today's match percentage is more than configured threshold higher than yesterday's
	increasePercent := ((todayMatchPercent - yesterdayMatchPercent) / yesterdayMatchPercent) * 100

	if increasePercent > config.DropRuleIncreaseThreshold {
		logger.GetLogger().Info("DROP rule match increase detected",
			zap.String("ruleId", vcRule.ID.String()),
			zap.String("ruleName", vcRule.Name),
			zap.Float64("yesterdayMatchPercent", yesterdayMatchPercent),
			zap.Float64("todayMatchPercent", todayMatchPercent),
			zap.Float64("increasePercent", increasePercent),
			zap.Float64("threshold", config.DropRuleIncreaseThreshold))
		return true
	}

	return false
}

// BuildAlert builds an alert specifically for DROP rule increase cases
func (p *DropRuleIncreaseProcessor) BuildAlert(vcAlert *DropRuleIncreaseAlert, config *VCAlertConfig) (*alerts_async.Alert, error) {
	title := fmt.Sprintf("DROP rule '%s' match rate increased significantly", vcAlert.RuleName)
	message := fmt.Sprintf("DROP rule '%s' in pipeline '%s' has increased its match rate by more than %.1f%% compared to yesterday. "+
		"Today: %s matched out of %s evaluated (%.2f%%), Yesterday: %s matched out of %s evaluated. "+
		"This indicates the rule is dropping more events than expected.",
		vcAlert.RuleName, vcAlert.Pipeline.Name, config.DropRuleIncreaseThreshold,
		util.HumanReadableNumber(vcAlert.TodayMatched), util.HumanReadableNumber(vcAlert.TodayEvaluated), vcAlert.MatchedPercent,
		util.HumanReadableNumber(vcAlert.YesterdayMatched), util.HumanReadableNumber(vcAlert.YesterdayEvaluated))

	return alerts_async.NewAlert(
		alerts_async.VolumeControlRule,
		alerts_async.WithEntity(vcAlert),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.VcDropRuleIncrease),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10006, "Volume control rule condition detected"),
	)
}
