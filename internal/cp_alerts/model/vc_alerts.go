package model

import (
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/pipeline"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/google/uuid"
)

// VCAlert represents a volume control rule alert
type VCAlert struct {
	Tenant             *tenant.Tenant     `json:"tenant"`
	Pipeline           *pipeline.Pipeline `json:"pipeline"`
	RuleID             uuid.UUID          `json:"rule_id"`
	RuleName           string             `json:"rule_name"`
	SourceID           uuid.UUID          `json:"source_id"`
	AlertType          VCAlertType        `json:"alert_type"`
	TodayMatched       int64              `json:"today_matched"`
	YesterdayMatched   int64              `json:"yesterday_matched"`
	TodayEvaluated     int64              `json:"today_evaluated"`
	YesterdayEvaluated int64              `json:"yesterday_evaluated"`
	MatchedPercent     float64            `json:"matched_percent"`
	UnmatchedPercent   float64            `json:"unmatched_percent"`
	DetectionTime      time.Time          `json:"detection_time"`
}

// VCAlertType represents the type of VC alert
type VCAlertType string

const (
	VCAlertTypeDropRuleIncrease          VCAlertType = "DROP_RULE_INCREASE"
	VCAlertTypePipelineDataReduction     VCAlertType = "PIPELINE_DATA_REDUCTION"
	VCAlertTypeUnmatchedNoRouteProcessor VCAlertType = "UNMATCHED_NO_ROUTE_PROCESSOR"
)

// GetEntityId returns the pipeline ID as the entity ID
func (vca VCAlert) GetEntityId() string {
	return vca.Pipeline.ID.String()
}

// GetEntityName returns the rule name in pipeline name format
func (vca VCAlert) GetEntityName() string {
	return vca.RuleName + " in " + vca.Pipeline.Name
}

// GetDataPlaneId returns the pipeline's data plane ID
func (vca VCAlert) GetDataPlaneId() string {
	return vca.Pipeline.DataPlaneID.String()
}

// GetTenantId returns the tenant ID
func (vca VCAlert) GetTenantId() string {
	return vca.Tenant.Id.String()
}

// GetSecondaryEntityId returns the rule ID as secondary entity
func (vca VCAlert) GetSecondaryEntityId() string {
	return vca.RuleID.String()
}

// NewVCAlert creates a new VC alert
func NewVCAlert(
	tenant *tenant.Tenant,
	pipeline *pipeline.Pipeline,
	ruleID uuid.UUID,
	ruleName string,
	sourceID uuid.UUID,
	alertType VCAlertType,
	todayMatched, yesterdayMatched, todayEvaluated, yesterdayEvaluated int64,
	matchedPercent, unmatchedPercent float64,
) *VCAlert {
	return &VCAlert{
		Tenant:             tenant,
		Pipeline:           pipeline,
		RuleID:             ruleID,
		RuleName:           ruleName,
		SourceID:           sourceID,
		AlertType:          alertType,
		TodayMatched:       todayMatched,
		YesterdayMatched:   yesterdayMatched,
		TodayEvaluated:     todayEvaluated,
		YesterdayEvaluated: yesterdayEvaluated,
		MatchedPercent:     matchedPercent,
		UnmatchedPercent:   unmatchedPercent,
		DetectionTime:      time.Now().UTC(),
	}
}

// NewPipelineVCAlert creates a new pipeline-level VC alert (for data reduction alerts)
func NewPipelineVCAlert(
	tenant *tenant.Tenant,
	pipeline *pipeline.Pipeline,
	alertType VCAlertType,
	todayIngested, yesterdayIngested, todayDelivered, yesterdayDelivered int64,
) *VCAlert {
	var reductionPercent float64
	if todayIngested > 0 {
		reductionPercent = ((float64(todayIngested) - float64(todayDelivered)) / float64(todayIngested)) * 100
	}

	// For pipeline alerts, use pipeline ID as both rule ID and source ID
	return &VCAlert{
		Tenant:             tenant,
		Pipeline:           pipeline,
		RuleID:             pipeline.ID,   // Use pipeline ID as rule ID for pipeline-level alerts
		RuleName:           pipeline.Name, // Use pipeline name as rule name
		SourceID:           pipeline.ID,   // Use pipeline ID as source ID for pipeline-level alerts
		AlertType:          alertType,
		TodayMatched:       todayDelivered, // In this context, "matched" means delivered
		YesterdayMatched:   yesterdayDelivered,
		TodayEvaluated:     todayIngested, // In this context, "evaluated" means ingested
		YesterdayEvaluated: yesterdayIngested,
		MatchedPercent:     100 - reductionPercent, // Delivery percentage
		UnmatchedPercent:   reductionPercent,       // Reduction percentage
		DetectionTime:      time.Now().UTC(),
	}
}
