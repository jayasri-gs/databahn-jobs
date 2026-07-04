package changeflag

type FlagGlobalDestination struct {
	Configuration   GlobalDestinationConfig `json:"config"`
	TenantId        string                  `json:"tenant_uuid"`
	DestinationId   string                  `json:"destination_id"`
	DestinationType string                  `json:"destination_type"`
}

type GlobalDestinationConfig struct {
	EventTypeMap          map[string]bool `json:"config"`
	UndeliveredPathFormat string          `json:"undeliveredPathFormat"`
}
