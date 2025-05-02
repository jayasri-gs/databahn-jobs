package changeflag

type FlagRouteProcessor struct {
	Id                       string            `json:"id"`
	UnmatchedAction          string            `json:"unmatched_action" faker:"oneof:PUBLISH,DROP"`
	ExplicitDropAction       string            `json:"explicit_drop_action" faker:"oneof:PUBLISH,DROP"`
	SecondaryDestinationId   string            `json:"secondary_destination_id"`
	SecondaryDestinationType string            `json:"secondary_destination_type"`
	PipelineId               string            `json:"pipeline_id"`
	TenantId                 string            `json:"tenant_id"`
	PrimaryDestinationId     string            `json:"primary_destination_id"`
	PrimaryDestinationType   string            `json:"primary_destination_type"`
	SourceId                 string            `json:"source_id"`
	IsVcRouteProcessor       bool              `json:"is_vc_route_processor"`
	IsOverride               bool              `json:"is_override"`
	OverrideDestinationId    string            `json:"override_destination_id"`
	OverrideDestinationType  string            `json:"override_destination_type"`
	OverrideConfiguration    map[string]string `json:"override_configuration"`
	SecretId                 string            `json:"secret_id"`
	OverrideForwardDataType  int               `json:"override_forward_data_type"`
}

func (frp FlagRouteProcessor) GetSecretId() string {
	return frp.SecretId
}

func (frp FlagRouteProcessor) AddConfig(extraConfig map[string]string) {
	for k, v := range extraConfig {
		frp.OverrideConfiguration[k] = v
	}
}
