package changeflag

type FlagDataReplay struct {
	Id                string            `json:"id"`
	TenantId          string            `json:"tenant_id"`
	ConnectorId       string            `json:"connector_id"`
	FleetId           string            `json:"fleet_id"`
	Action            string            `json:"action"`
	Source            string            `json:"source"`
	Destination       string            `json:"destination"`
	RequestId         string            `json:"request_id"`
	BucketName        string            `json:"bucket_name"`
	BucketPrefix      string            `json:"bucket_prefix"`
	AccessKeyId       string            `json:"access_key_id"`
	SecretAccessKey   string            `json:"secret_access_key"`
	Region            string            `json:"region"`
	FileName          string            `json:"file_name"`
	DeviceType        string            `json:"device_type"`
	DeviceVendor      string            `json:"device_vendor"`
	LogType           string            `json:"log_type"`
	ReplayType        string            `json:"replay_type"`
	SourceName        string            `json:"source_name"`
	DataStore         string            `json:"data_store"`
	AdditionalConfig  map[string]string `json:"additional_config"`
	AdditionalHeaders map[string]string `json:"additional_headers"`
}
