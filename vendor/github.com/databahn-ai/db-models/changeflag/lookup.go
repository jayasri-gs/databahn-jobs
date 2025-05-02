package changeflag

// FlagLookup change flag model for lookup
type FlagLookup struct {
	Id        string `json:"id"`
	TenantId  string `json:"tenant_id"`
	RequestId string `json:"request_id"`
}
