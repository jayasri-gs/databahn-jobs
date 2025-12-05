package changeflag

type PreProcessingFunctionType string

type DataType string

const (
	PreProcessingFunctionTypeGrok    PreProcessingFunctionType = "GROK"
	PreProcessingFunctionTypeReplace PreProcessingFunctionType = "REPLACE"
)

const (
	DataTypeJSON     DataType = "JSON"
	DataTypeKeyValue DataType = "KEY_VALUE"
	DataTypeXML      DataType = "XML"
	DataTypeCEF      DataType = "CEF"
	DataTypeCSV      DataType = "CSV"
	DataTypePSV      DataType = "PSV"
	DataTypeLEEF     DataType = "LEEF"
)

type FlagSource struct {
	Id                     string                   `json:"id"`
	TenantId               string                   `json:"tenant_id"`
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
	SchemaLessEnabled      bool                     `json:"schemaless_enabled"`
	SchemaLessConfig       *SchemaLessConfig        `json:"schemaless_configuration"`
}

type FlagSourceAdvancedConfig struct {
	SendUnmatchedEventToPrimaryDestination bool `json:"sendUnmatchedEventToPrimaryDestination"`
}

type SchemaLessConfig struct {
	PreProcessor PreProcessor `json:"preProcessor"`
	Parser       ParserConfig `json:"parser"`
}

type ParserConfig struct {
	DataType       DataType        `json:"dataType,omitempty"`       // Data type: "json", "keyvalue", etc.
	KeyValueConfig *KeyValueConfig `json:"keyValueConfig,omitempty"` // Configuration for key-value data type
	LeefConfig     *LeefConfig     `json:"leefConfig,omitempty"`     // Configuration for LEF data type
}

// KeyValueConfig holds configuration for key-value data type parsing
type KeyValueConfig struct {
	FieldDelimiter    string `json:"fieldDelimiter"`    // Delimiter between fields (e.g., "\n", "|", " ")
	KeyValueDelimiter string `json:"keyValueDelimiter"` // Delimiter between key and value (e.g., "=", ":", " ")
}

// LeefConfig holds configuration for LEEF data type parsing
type LeefConfig struct {
	FieldDelimiter string `json:"fieldDelimiter"` // Delimiter between fields (e.g., "\n", "|", " ")
}

type PreProcessor struct {
	Enabled           bool                    `json:"enabled"`
	DataExtractionOps []PreProcessingFunction `json:"dataExtractionOps"`
}

type PreProcessingFunction struct {
	Type PreProcessingFunctionType `json:"type"`

	GrokFunction    *GrokFunction    `json:"grokFunction,omitempty"`
	ReplaceFunction *ReplaceFunction `json:"replaceFunction,omitempty"`
}

type GrokFunction struct {
	Patterns []string `json:"patterns"`
}

type ReplaceFunction struct {
	ReplacePattern string `json:"replacePattern"`
	ReplaceWith    string `json:"replaceWith"`
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
