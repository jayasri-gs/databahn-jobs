package changeflag

import (
	"github.com/databahn-ai/db-models/rule"
)

// FlagRule change flag model for rule
type FlagRule struct {
	Id                   string                 `json:"id"`
	Name                 string                 `json:"name"`
	Description          string                 `json:"description"`
	TenantId             string                 `json:"tenantId"`
	PipelineId           string                 `json:"pipeline_id"`
	OldPipelineId        string                 `json:"old_pipeline_id"`
	DestinationId        string                 `json:"destination_id"`
	Scope                string                 `json:"scope"`
	Priority             int                    `json:"priority"`
	ActionType           string                 `json:"action_type"`
	Type                 string                 `json:"type"`
	Tags                 []FlagTag              `json:"tags"`
	SamplingRate         int64                  `json:"sampling_rate"`
	DestinationType      string                 `json:"destination_type"`
	EventSourceId        string                 `json:"event_source_id"`
	RuleFilterQuery      string                 `json:"rule_filter_query"`
	AggregationConfig    rule.AggregationConfig `json:"aggregation_config"`
	SuppressionConfig    rule.SuppressionConfig `json:"suppression_config"`
	ReferencedAttributes []string               `json:"referenced_attributes"`
}

type FlagTag struct {
	Id         int
	EntityType string
	Tag        string
}
