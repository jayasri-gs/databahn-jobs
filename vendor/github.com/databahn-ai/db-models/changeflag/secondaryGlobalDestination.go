package changeflag

type FlagSecondaryGlobalDestination struct {
	ID                           string            `json:"id"`
	Name                         string            `json:"name"`
	Description                  string            `json:"description"`
	HistoryVersion               int               `json:"history_version"`
	TenantUUID                   string            `json:"tenant_uuid"`
	DestinationType              string            `json:"destination_type"`
	ForwardDataType              int               `json:"forward_data_type"`
	Scope                        string            `json:"scope"`
	Config                       map[string]string `json:"config"`
	PipelineId                   string            `json:"pipeline_id"`
	IsSecondaryGlobalDestination bool              `json:"is_secondary_global_destination"`
}
