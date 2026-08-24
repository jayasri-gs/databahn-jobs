package changeflag

type FlagInsightRule struct {
	Id              string            `json:"id"`
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	SourceId        string            `json:"source_id"`
	TenantId        string            `json:"tenant_id"`
	RuleFilterQuery string            `json:"rule_filter_query"`
	Attributes      []string          `json:"attributes"`
	LookupId        string            `json:"lookup_id"`
	DefineLookup    bool              `json:"define_lookup"`
	RenameMap       map[string]string `json:"rename_map,omitempty"`
}
