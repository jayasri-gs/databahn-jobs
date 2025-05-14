package changeflag

type FlagCustomNormalization struct {
	Id             string      `json:"id"`
	TransformLogic string      `json:"transformLogic"`
	TenantId       string      `json:"tenant_id"`
	CustomerId     interface{} `json:"customer_id"`
	SourceId       string      `json:"source_id"`
	Action         string      `json:"action"`
}
