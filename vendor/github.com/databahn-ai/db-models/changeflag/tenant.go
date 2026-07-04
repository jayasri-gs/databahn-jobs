package changeflag

type FlagTenant struct {
	TenantId        string `json:"tenant_id"`
	Name            string `json:"name"`
	Active          bool   `json:"active"`
	BackendClientId string `json:"backend_client_id"`
}
