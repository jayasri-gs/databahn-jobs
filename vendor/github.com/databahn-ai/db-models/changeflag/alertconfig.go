package changeflag

// FlagAlertConfig represents an alert configuration change flag.
// Used for propagating alert configuration changes (like TRANSFORMATION_FIELDS_DROP) to services.
type FlagAlertConfig struct {
	Id         string           `json:"id"`
	TenantId   string           `json:"tenant_id"`
	EntityId   string           `json:"entity_id"`
	EntityType string           `json:"entity_type"`
	AlertType  string           `json:"alert_type"`
	Config     *AlertConfigData `json:"config"`
}

// AlertConfigData contains the alert configuration settings
type AlertConfigData struct {
	Enabled                             bool                                 `json:"enabled"`
	TransformationFieldsDropAlertConfig *TransformationFieldsDropAlertConfig `json:"transformationFieldsDropAlertConfig,omitempty"`
}

// TransformationFieldsDropAlertConfig contains settings for transformation fields drop alert
type TransformationFieldsDropAlertConfig struct {
	DropThresholdPercent  float64 `json:"dropThresholdPercent"`
	MinEventCountForAlert int64   `json:"minEventCountForAlert"`
}
