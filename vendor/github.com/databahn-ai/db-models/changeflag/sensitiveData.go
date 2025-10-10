package changeflag

type FlagSensitiveDataConfig struct {
	Id       string                 `json:"id"`
	TenantId string                 `json:"tenant_id"`
	SourceId string                 `json:"source_id"`
	Function string                 `json:"function"`
	Patterns []SensitiveDataPattern `json:"patterns"`
	Tag      string                 `json:"tag"`
}

type SensitiveDataPattern struct {
	Id      string `json:"id"`
	Name    string `json:"name"`
	Type    string `json:"type"`
	Pattern string `json:"pattern"`
}
