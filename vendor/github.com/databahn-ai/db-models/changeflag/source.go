package changeflag

type FlagSource struct {
	Id            string            `json:"id"`
	TenantId      string            `json:"tenant_id"`
	ConnectorId   string            `json:"connector_id"`
	EdgeId        string            `json:"edge_id"`
	Name          string            `json:"name"`
	Scope         string            `json:"scope"`
	Type          string            `json:"type"`
	Device        string            `json:"device"`
	Vendor        string            `json:"vendor"`
	Version       string            `json:"version"`
	Status        int               `json:"status"`
	Config        map[string]string `json:"config"`
	SecretId      string            `json:"secret_id"`
	PullMechanism string            `json:"pullMechanism"`
}

func (fd FlagSource) GetSecretId() string {
	return fd.SecretId
}

func (fd FlagSource) AddConfig(extraConfig map[string]string) {
	for k, v := range extraConfig {
		fd.Config[k] = v
	}
}
