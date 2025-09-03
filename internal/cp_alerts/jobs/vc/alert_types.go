package vc

import (
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/google/uuid"
)

// DropRuleIncreaseAlert represents a DROP rule increase alert
type DropRuleIncreaseAlert struct {
	Tenant             *tenant.Tenant     `json:"tenant"`
	Pipeline           *pipeline.Pipeline `json:"pipeline"`
	RuleID             uuid.UUID          `json:"rule_id"`
	RuleName           string             `json:"rule_name"`
	SourceID           uuid.UUID          `json:"source_id"`
	TodayMatched       int64              `json:"today_matched"`
	YesterdayMatched   int64              `json:"yesterday_matched"`
	TodayEvaluated     int64              `json:"today_evaluated"`
	YesterdayEvaluated int64              `json:"yesterday_evaluated"`
	MatchedPercent     float64            `json:"matched_percent"`
	UnmatchedPercent   float64            `json:"unmatched_percent"`
	DetectionTime      time.Time          `json:"detection_time"`
}

// PipelineDataReductionAlert represents a pipeline data reduction alert
type PipelineDataReductionAlert struct {
	Tenant             *tenant.Tenant     `json:"tenant"`
	Pipeline           *pipeline.Pipeline `json:"pipeline"`
	TodayIngested      int64              `json:"today_ingested"`
	YesterdayIngested  int64              `json:"yesterday_ingested"`
	TodayDelivered     int64              `json:"today_delivered"`
	YesterdayDelivered int64              `json:"yesterday_delivered"`
	ReductionPercent   float64            `json:"reduction_percent"`
	DetectionTime      time.Time          `json:"detection_time"`
}

// UnmatchedNoRouteProcessorAlert represents an unmatched no route processor alert
type UnmatchedNoRouteProcessorAlert struct {
	Tenant           *tenant.Tenant     `json:"tenant"`
	Pipeline         *pipeline.Pipeline `json:"pipeline"`
	RuleID           uuid.UUID          `json:"rule_id"`
	RuleName         string             `json:"rule_name"`
	SourceID         uuid.UUID          `json:"source_id"`
	TodayEvaluated   int64              `json:"today_evaluated"`
	TodayMatched     int64              `json:"today_matched"`
	MatchedPercent   float64            `json:"matched_percent"`
	UnmatchedPercent float64            `json:"unmatched_percent"`
	DetectionTime    time.Time          `json:"detection_time"`
}

// DropRuleIncreaseAlert interface implementations
func (d *DropRuleIncreaseAlert) GetEntityId() string          { return d.RuleID.String() }
func (d *DropRuleIncreaseAlert) GetEntityName() string        { return d.RuleName + " in " + d.Pipeline.Name }
func (d *DropRuleIncreaseAlert) GetDataPlaneId() string       { return d.Pipeline.DataPlaneID.String() }
func (d *DropRuleIncreaseAlert) GetTenantId() string          { return d.Tenant.Id.String() }
func (d *DropRuleIncreaseAlert) GetSecondaryEntityId() string { return d.Pipeline.ID.String() }

// PipelineDataReductionAlert interface implementations
func (p *PipelineDataReductionAlert) GetEntityId() string          { return p.Pipeline.ID.String() }
func (p *PipelineDataReductionAlert) GetEntityName() string        { return p.Pipeline.Name }
func (p *PipelineDataReductionAlert) GetDataPlaneId() string       { return p.Pipeline.DataPlaneID.String() }
func (p *PipelineDataReductionAlert) GetTenantId() string          { return p.Tenant.Id.String() }
func (p *PipelineDataReductionAlert) GetSecondaryEntityId() string { return p.Pipeline.ID.String() }

// UnmatchedNoRouteProcessorAlert interface implementations
func (u *UnmatchedNoRouteProcessorAlert) GetEntityId() string { return u.RuleID.String() }
func (u *UnmatchedNoRouteProcessorAlert) GetEntityName() string {
	return u.RuleName + " in " + u.Pipeline.Name
}
func (u *UnmatchedNoRouteProcessorAlert) GetDataPlaneId() string {
	return u.Pipeline.DataPlaneID.String()
}
func (u *UnmatchedNoRouteProcessorAlert) GetTenantId() string          { return u.Tenant.Id.String() }
func (u *UnmatchedNoRouteProcessorAlert) GetSecondaryEntityId() string { return u.RuleID.String() }

// Constructor functions
func NewDropRuleIncreaseAlert(
	tenant *tenant.Tenant,
	pipeline *pipeline.Pipeline,
	ruleID uuid.UUID,
	ruleName string,
	sourceID uuid.UUID,
	todayMatched, yesterdayMatched, todayEvaluated, yesterdayEvaluated int64,
	matchedPercent, unmatchedPercent float64,
) *DropRuleIncreaseAlert {
	return &DropRuleIncreaseAlert{
		Tenant:             tenant,
		Pipeline:           pipeline,
		RuleID:             ruleID,
		RuleName:           ruleName,
		SourceID:           sourceID,
		TodayMatched:       todayMatched,
		YesterdayMatched:   yesterdayMatched,
		TodayEvaluated:     todayEvaluated,
		YesterdayEvaluated: yesterdayEvaluated,
		MatchedPercent:     matchedPercent,
		UnmatchedPercent:   unmatchedPercent,
		DetectionTime:      time.Now().UTC(),
	}
}

func NewPipelineDataReductionAlert(
	tenant *tenant.Tenant,
	pipeline *pipeline.Pipeline,
	todayIngested, yesterdayIngested, todayDelivered, yesterdayDelivered int64,
) *PipelineDataReductionAlert {
	var reductionPercent float64
	if todayIngested > 0 {
		reductionPercent = ((float64(todayIngested) - float64(todayDelivered)) / float64(todayIngested)) * 100
	}

	return &PipelineDataReductionAlert{
		Tenant:             tenant,
		Pipeline:           pipeline,
		TodayIngested:      todayIngested,
		YesterdayIngested:  yesterdayIngested,
		TodayDelivered:     todayDelivered,
		YesterdayDelivered: yesterdayDelivered,
		ReductionPercent:   reductionPercent,
		DetectionTime:      time.Now().UTC(),
	}
}

func NewUnmatchedNoRouteProcessorAlert(
	tenant *tenant.Tenant,
	pipeline *pipeline.Pipeline,
	ruleID uuid.UUID,
	ruleName string,
	sourceID uuid.UUID,
	todayEvaluated, todayMatched int64,
	matchedPercent, unmatchedPercent float64,
) *UnmatchedNoRouteProcessorAlert {
	return &UnmatchedNoRouteProcessorAlert{
		Tenant:           tenant,
		Pipeline:         pipeline,
		RuleID:           ruleID,
		RuleName:         ruleName,
		SourceID:         sourceID,
		TodayEvaluated:   todayEvaluated,
		TodayMatched:     todayMatched,
		MatchedPercent:   matchedPercent,
		UnmatchedPercent: unmatchedPercent,
		DetectionTime:    time.Now().UTC(),
	}
}
