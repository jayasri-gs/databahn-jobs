package changeflag

type ChangeFlag struct {
	EntityId   string `json:"entity_id"`
	RequestId  string `json:"request_id"`
	TenantId   string `json:"tenant_id"`
	EntityType string `json:"entity_type"`
	Action     string `json:"action"`
	Entity     []byte `json:"entity"`
}
