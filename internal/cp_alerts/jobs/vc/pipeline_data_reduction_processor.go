package vc

import (
	"context"
	"fmt"

	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

// PipelineDataReductionProcessor handles pipeline data reduction alerts
type PipelineDataReductionProcessor struct{}

func (p *PipelineDataReductionProcessor) GetAlertType() string {
	return "PipelineDataReduction"
}

func (p *PipelineDataReductionProcessor) ProcessAlerts(_ context.Context, data *ProcessingData) ([]*alerts_async.Alert, []string, error) {
	var alerts []*alerts_async.Alert
	var healthyRuleIds []string

	// Only process if pipeline has rules and log sources
	if len(data.VCRules) == 0 {
		logger.GetLogger().Debug("skipping pipeline data reduction check - no rules",
			zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()),
			zap.Int("rules", len(data.VCRules)))
		return alerts, healthyRuleIds, nil
	}

	logger.GetLogger().Debug("Processing pipeline data reduction alerts",
		zap.String("tenantId", data.Tenant.Id.String()),
		zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()))

	shouldAlert, todayIngested, todayDelivered, yesterdayIngested, yesterdayDelivered, err := p.checkPipelineDataReductionCondition(data)
	if err != nil {
		return nil, nil, fmt.Errorf("error checking pipeline data reduction: %w", err)
	}

	if shouldAlert {
		vcAlert := NewPipelineDataReductionAlert(data.Tenant, &data.PipelineMapping.Pipeline,
			todayIngested, yesterdayIngested, todayDelivered, yesterdayDelivered)

		alert, err := p.BuildAlert(vcAlert, data.Config)
		if err != nil {
			return nil, nil, fmt.Errorf("error building pipeline data reduction alert: %w", err)
		}

		alerts = append(alerts, alert)

		logger.GetLogger().Info("Pipeline data reduction alert created",
			zap.String("tenantId", data.Tenant.Id.String()),
			zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()))
	} else {
		// Only auto-resolve if pipeline has sufficient traffic to make a reliable assessment
		if todayIngested >= data.Config.MinimumEventsThreshold {
			// For pipeline-level alerts, the "rule ID" is actually the pipeline ID
			healthyRuleIds = append(healthyRuleIds, data.PipelineMapping.Pipeline.ID.String())
			logger.GetLogger().Info("Pipeline is healthy with sufficient traffic, adding to auto-resolution list",
				zap.String("tenantId", data.Tenant.Id.String()),
				zap.String("pipelineId", data.PipelineMapping.Pipeline.ID.String()),
				zap.String("pipelineName", data.PipelineMapping.Pipeline.Name),
				zap.Int64("todayIngested", todayIngested),
				zap.Int64("minimumThreshold", data.Config.MinimumEventsThreshold))
		}
	}

	return alerts, healthyRuleIds, nil
}

// checkPipelineDataReductionCondition checks if pipeline has excessive data reduction
func (p *PipelineDataReductionProcessor) checkPipelineDataReductionCondition(data *ProcessingData) (bool, int64, int64, int64, int64, error) {
	pipelineId := data.PipelineMapping.Pipeline.ID.String()
	logSourceId := data.PipelineMapping.LogSourceID.String()

	// Get today's pipeline statistics
	todayStats, err := data.StatsService.GetPipelineStats(pipelineId, logSourceId, data.Config.TodayStart, data.Config.TodayEnd)
	if err != nil {
		return false, 0, 0, 0, 0, err
	}

	// Get yesterday's pipeline statistics
	yesterdayStats, err := data.StatsService.GetPipelineStats(pipelineId, logSourceId, data.Config.YesterdayStart, data.Config.YesterdayEnd)
	if err != nil {
		return false, 0, 0, 0, 0, err
	}

	// Only alert if there's sufficient traffic
	if todayStats.Ingested < data.Config.MinimumEventsThreshold {
		logger.GetLogger().Debug("skipping pipeline data reduction alert due to low traffic",
			zap.String("pipelineId", pipelineId),
			zap.Int64("todayIngested", todayStats.Ingested),
			zap.Int64("minimumThreshold", data.Config.MinimumEventsThreshold))
		return false, todayStats.Ingested, todayStats.Delivered, yesterdayStats.Ingested, yesterdayStats.Delivered, nil
	}

	// Calculate reduction percentages
	var todayReductionPercent, yesterdayReductionPercent float64
	if todayStats.Ingested > 0 {
		todayReductionPercent = ((float64(todayStats.Ingested) - float64(todayStats.Delivered)) / float64(todayStats.Ingested)) * 100
	}
	if yesterdayStats.Ingested > 0 {
		yesterdayReductionPercent = ((float64(yesterdayStats.Ingested) - float64(yesterdayStats.Delivered)) / float64(yesterdayStats.Ingested)) * 100
	}

	// Check if today's reduction is significantly higher than yesterday's
	if yesterdayReductionPercent > 0 {
		reductionIncrease := ((todayReductionPercent - yesterdayReductionPercent) / yesterdayReductionPercent) * 100

		if reductionIncrease > data.Config.PipelineDataReductionThreshold {
			logger.GetLogger().Info("Pipeline data reduction increase detected",
				zap.String("pipelineId", pipelineId),
				zap.Float64("todayReductionPercent", todayReductionPercent),
				zap.Float64("yesterdayReductionPercent", yesterdayReductionPercent),
				zap.Float64("reductionIncrease", reductionIncrease),
				zap.Float64("threshold", data.Config.PipelineDataReductionThreshold))
			return true, todayStats.Ingested, todayStats.Delivered, yesterdayStats.Ingested, yesterdayStats.Delivered, nil
		}
	}

	return false, todayStats.Ingested, todayStats.Delivered, yesterdayStats.Ingested, yesterdayStats.Delivered, nil
}

// BuildAlert builds an alert specifically for pipeline data reduction cases
func (p *PipelineDataReductionProcessor) BuildAlert(vcAlert *PipelineDataReductionAlert, config *VCAlertConfig) (*alerts_async.Alert, error) {
	title := fmt.Sprintf("Volume Controller: Pipeline '%s' has excessive data reduction", vcAlert.Pipeline.Name)
	message := fmt.Sprintf(
		"Pipeline '%s' is reducing data by %.2f%% today (%s, %s delivered out of %s ingested), "+
			"which is more than %.1f%% higher than yesterday (%s, %s delivered out of %s ingested). "+
			"This indicates volume control rules are dropping significantly more events than normal.",
		vcAlert.Pipeline.Name, vcAlert.ReductionPercent, config.TodayStart.Format("2006-01-02"), util.HumanReadableNumber(vcAlert.TodayDelivered), util.HumanReadableNumber(vcAlert.TodayIngested),
		config.PipelineDataReductionThreshold, config.YesterdayStart.Format("2006-01-02"), util.HumanReadableNumber(vcAlert.YesterdayDelivered), util.HumanReadableNumber(vcAlert.YesterdayIngested),
	)

	return alerts_async.NewAlert(
		alerts_async.VolumeControlRule,
		alerts_async.WithEntity(vcAlert),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.VcPipelineDataReduction),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10006, "Volume control rule condition detected"),
	)
}
