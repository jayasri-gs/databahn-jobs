package changeflag

type FlagDestination struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	HistoryVersion      int               `json:"history_version"`
	TenantUUID          string            `json:"tenant_uuid"`
	DestinationType     string            `json:"destination_type"`
	ForwardDataType     int               `json:"forward_data_type"`
	Scope               string            `json:"scope"`
	Config              map[string]string `json:"config"`
	SecretId            string            `json:"secret_id"`
	PipelineId          string            `json:"pipeline_id"`
	DestinationOverride bool              `json:"destination_override"`
	BackupDestinationId string            `json:"backup_destination_id"`
}

func (fd FlagDestination) GetSecretId() string {
	return fd.SecretId
}

func (fd FlagDestination) AddConfig(extraConfig map[string]string) {
	for k, v := range extraConfig {
		fd.Config[k] = v
	}
}
