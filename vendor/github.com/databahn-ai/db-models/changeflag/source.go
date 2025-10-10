package changeflag

type FlagSource struct {
	Id                     string                   `json:"id"`
	TenantId               string                   `json:"tenant_id"`
	ConnectorId            string                   `json:"connector_id"`
	EdgeId                 string                   `json:"edge_id"`
	Name                   string                   `json:"name"`
	Scope                  string                   `json:"scope"`
	Type                   string                   `json:"type"`
	Device                 string                   `json:"device"`
	Vendor                 string                   `json:"vendor"`
	Version                string                   `json:"version"`
	Status                 int                      `json:"status"`
	Config                 map[string]string        `json:"config"`
	SecretId               string                   `json:"secret_id"`
	PullMechanism          string                   `json:"pullMechanism"`
	AdvancedConfig         FlagSourceAdvancedConfig `json:"advanced_configuration"`
	ApplicationName        string                   `json:"application_name"`
	UnparsedFlowEnabled    bool                     `json:"unparsedFlowEnabled"`
	DeviceInventoryEnabled bool                     `json:"device_inventory_enabled"`
	Filter                 *RuleGroup               `json:"filter"`
}

type FlagSourceAdvancedConfig struct {
	SendUnmatchedEventToPrimaryDestination bool `json:"sendUnmatchedEventToPrimaryDestination"`
}

func (fd FlagSource) GetSecretId() string {
	return fd.SecretId
}

func (fd FlagSource) AddConfig(extraConfig map[string]string) {
	for k, v := range extraConfig {
		fd.Config[k] = v
	}
}

type RuleGroup struct {
	Rules      []RuleItem `json:"rules"`
	Combinator string     `json:"combinator"`
}

type RuleItem struct {
	// For simple rules
	Field    string `json:"field"`
	Value    string `json:"value"`
	Operator string `json:"operator"`

	// For nested groups
	Rules      []RuleItem `json:"rules"`
	Combinator string     `json:"combinator"`
}

func (r RuleItem) IsNestedGroup() bool {
	return len(r.Rules) > 0
}
