package changeflag

type TransformationOpType string

const (
	Replace                           TransformationOpType = "REPLACE"
	ConstantReplace                   TransformationOpType = "CONSTANT_REPLACE"
	SubstringExtraction               TransformationOpType = "SUBSTRING_EXTRACTION"
	Split                             TransformationOpType = "SPLIT"
	Trim                              TransformationOpType = "TRIM"
	Numerify                          TransformationOpType = "NUMERIFY"
	Mask                              TransformationOpType = "MASK"
	StringTruncate                    TransformationOpType = "STRING_TRUNCATE"
	StringRedact                      TransformationOpType = "STRING_REDACT"
	StringBase64Encode                TransformationOpType = "STRING_BASE64_ENCODE"
	StringBase64Decode                TransformationOpType = "STRING_BASE64_DECODE"
	StringMD5                         TransformationOpType = "STRING_MD5"
	TimeFromUnixTimestampSeconds      TransformationOpType = "TIME_FROM_UNIX_TIMESTAMP_SECONDS"
	TimeFromUnixTimestampMilliseconds TransformationOpType = "TIME_FROM_UNIX_TIMESTAMP_MILLISECONDS"
	TimeUnixTsSec                     TransformationOpType = "TIME_UNIX_TS_SEC"
	TimeUnixTsMs                      TransformationOpType = "TIME_UNIX_TS_MS"
	Case                              TransformationOpType = "CASE"
	NullIf                            TransformationOpType = "NULL_IF"
	Integerify                        TransformationOpType = "INTEGERIFY"
	Stringify                         TransformationOpType = "STRINGIFY"
	Booleanify                        TransformationOpType = "BOOLEANIFY"

	JsonExtraction     TransformationOpType = "JSON_EXTRACT"
	XmlExtraction      TransformationOpType = "XML_EXTRACT"
	KeyValueExtraction TransformationOpType = "KEY_VALUE_EXTRACT"
	CsvExtraction      TransformationOpType = "CSV_EXTRACT"

	StringLength           TransformationOpType = "STRING_LENGTH"
	ArrayLength            TransformationOpType = "ARRAY_LENGTH"
	StringConcat           TransformationOpType = "STRING_CONCAT"
	StartsWith             TransformationOpType = "STARTS_WITH"
	PercentDecode          TransformationOpType = "PERCENT_DECODE"
	TimeNow                TransformationOpType = "TIME_NOW"
	ParseTimestamp         TransformationOpType = "PARSE_TIMESTAMP"
	JoinStringArray        TransformationOpType = "JOIN_STRING_ARRAY"
	JoinObjectArrayKVPairs TransformationOpType = "JOIN_OBJECT_ARRAY_KV_PAIRS"
	AdvancedCase           TransformationOpType = "ADVANCED_CASE"
)

type FlagTransform struct {
	ID                            string                `json:"id"`
	Name                          string                `json:"name"`
	SourceId                      string                `json:"source_id"`
	DestinationId                 string                `json:"destination_id"`
	DestinationType               string                `json:"destination_type"`
	PipelineId                    string                `json:"pipeline_id"`
	TenantId                      string                `json:"tenant_id"`
	Status                        int                   `json:"status"`
	TransformationType            string                `json:"transformation_type"`
	TransformationOutput          string                `json:"transformation_output"`
	TransformationDestinationType string                `json:"transformation_destination_type"`
	IncludeRawEvent               bool                  `json:"include_raw_event"`
	Function                      FlagTransformFunction `json:"function,omitempty"`
	Device                        string                `json:"device"`
	Vendor                        string                `json:"vendor"`
	LogType                       string                `json:"log_type"`
	SensitiveDataDetectionEnabled bool                  `json:"sensitive_data_detection_enabled"`
	SensitiveDataEncryptionType   string                `json:"sensitive_data_encryption_type"`
	AdditionalConfig              map[string]string     `json:"additional_config"`
}

type FlagTransformFunction struct {
	Type                     string                     `json:"type"`
	RenameConfig             RenameConfiguration        `json:"rename_config"`
	ReorderConfig            ReorderConfig              `json:"reorder_config"`
	ResizeConfig             ResizeConfig               `json:"resize_config"`
	OcsfTransformationConfig OcsfTransformationMappings `json:"ocsf_transformation_config"`
	CodeBlockConfig          CodeBlockConfig            `json:"code_block_config"`
	RegexExtractConfig       string                     `json:"regex_extract_config"`
}

type CodeBlockConfig struct {
	SampleInput string
	CodeBlock   string
}
type OcsfTransformationMappings struct {
	OcsfTransformationBlocks []OcsfTransformationBlock
	GetFlattenedOutput       bool `json:"getFlattenedOutput"`
}
type ReorderConfig struct {
	Fields []string `json:"fields"`
}
type ResizeConfig struct {
	FieldsToInclude []string `json:"fieldsToInclude"`
}
type RenameConfig struct {
	RenameFields []RenameFields `json:"renameFields"`
}
type RenameConfiguration struct {
	RenameFields  []RenameFieldsWithOperators `json:"renameFields"`
	DerivedFields []DerivedField              `json:"derivedFields"`
	// Deprecated: use customTransformationConfig instead
	MessageReformattingEnabled bool `json:"messageReformattingEnabled"`
	// Deprecated: use customTransformationConfig instead
	MessageReformattingTemplate string                     `json:"messageReformattingTemplate"`
	MessageReformattingSegments []Segment                  `json:"messageReformattingSegments"`
	CustomTransformationConfig  CustomTransformationConfig `json:"customTransformationConfig"`
}
type CustomTransformationConfig struct {
	CustomTransformationBlocks []CustomTransformationBlock `json:"customTransformationBlocks"`
}

type CustomTransformationBlock struct {
	CustomFilterCriteria        OcsfFilterCriteria `json:"customFilterCriteria"`
	MessageReformattingTemplate string             `json:"messageReformattingTemplate"`
	MessageReformattingSegments []Segment          `json:"messageReformattingSegments"`
}

type Segment struct {
	Type         string `json:"type"`
	Value        string `json:"value"`
	VariableName string `json:"variableName,omitempty"`
}
type DerivedField struct {
	Include       bool                     `json:"include"`
	AttributeName string                   `json:"attributeName"`
	SourceField   SourceField              `json:"sourceField"`
	Operators     []TransformationOperator `json:"operators"`
}
type SourceField struct {
	DatabahnAttribute string `json:"databahnAttribute"`
}
type TrimConfig struct {
	StripLeading  bool `json:"stripLeading"`
	StripTrailing bool `json:"stripTrailing"`
}
type TransformationOperator struct {
	Type TransformationOpType `json:"type"`

	ReplaceConfig                  *ReplaceConfig                `json:"replaceConfig,omitempty"`
	TrimConfig                     *TrimConfig                   `json:"trimConfig,omitempty"`
	MaskConfig                     *MaskConfig                   `json:"maskConfig,omitempty"`
	SubstringExtractionConfig      *SubstringExtractionConfig    `json:"substringExtractionConfig,omitempty"`
	JsonExtractionConfig           *JsonExtractionConfig         `json:"jsonExtractionConfig,omitempty"`
	XmlExtractionConfig            *XmlExtractionConfig          `json:"xmlExtractionConfig,omitempty"`
	ParseKeyValueConfig            *ParseKeyValueConfig          `json:"parseKeyValueConfig,omitempty"`
	ParseCsvConfig                 *ParseCsvConfig               `json:"parseCsvConfig,omitempty"`
	ConstantReplaceConfig          *ConstantReplaceConfig        `json:"constantReplaceConfig,omitempty"`
	SplitOperatorConfig            *SplitOperatorConfig          `json:"splitOperatorConfig,omitempty"`
	StringRedactOperatorConfig     *StringRedactOperatorConfig   `json:"stringRedactOperatorConfig,omitempty"`
	StringTruncateOperatorConfig   *StringTruncateOperatorConfig `json:"stringTruncateOperatorConfig,omitempty"`
	CaseOperatorConfig             *CaseOperatorConfig           `json:"caseOperatorConfig,omitempty"`
	TimeNowConfig                  *TimeNowConfig                `json:"timeNowConfig,omitempty"`
	TimeFromUnixSecondsConfig      *TimeNowConfig                `json:"timeFromUnixSecondsConfig,omitempty"`
	TimeFromUnixMillisecondsConfig *TimeNowConfig                `json:"timeFromUnixMillisecondsConfig,omitempty"`
	StringConcatConfig             *StringConcatConfig           `json:"stringConcatConfig,omitempty"`
	StartsWithConfig               *StartsWithConfig             `json:"startsWithConfig,omitempty"`
	NullIfOperatorConfig           *NullIfOperatorConfig         `json:"nullIfOperatorConfig,omitempty"`
	ParseTimestampConfig           *ParseTimestampConfig         `json:"parseTimestampConfig,omitempty"`
	JoinStringArrayConfig          *JoinStringArrayConfig        `json:"joinStringArrayConfig,omitempty"`
	JoinObjectArrayKVPairsConfig   *JoinObjectArrayKVPairsConfig `json:"joinObjectArrayKVPairsConfig,omitempty"`
	AdvancedCaseOperatorConfig     *AdvancedCaseOperatorConfig   `json:"advancedCaseOperatorConfig,omitempty"`
}

type RenameFields struct {
	LogAttribute            string        `json:"logAttribute"`
	DatabahnAttribute       string        `json:"databahnAttribute"`
	RenameAttribute         string        `json:"renameAttribute"`
	Include                 bool          `json:"include"`
	ReplaceOperatorEnabled  bool          `json:"replaceOperatorEnabled"`
	ReplaceConfig           ReplaceConfig `json:"replaceConfig"`
	TrimOperatorEnabled     bool          `json:"trimOperatorEnabled"`
	NumerifyOperatorEnabled bool          `json:"numerifyOperatorEnabled"`
	MaskOperatorEnabled     bool          `json:"maskOperatorEnabled"`
	MaskConfig              MaskConfig    `json:"maskConfig"`

	SubstringExtractionOperatorEnabled bool                      `json:"substringExtractionOperatorEnabled"`
	SubstringExtractionConfig          SubstringExtractionConfig `json:"substringExtractionConfig"`
	JsonExtractionOperatorEnabled      bool                      `json:"jsonExtractionOperatorEnabled"`
	JsonExtractionConfig               JsonExtractionConfig      `json:"jsonExtractionConfig"`
	XmlExtractionOperatorEnabled       bool                      `json:"xmlExtractionOperatorEnabled"`
	XmlExtractionConfig                XmlExtractionConfig       `json:"xmlExtractionConfig"`
	KeyValueExtractionOperatorEnabled  bool                      `json:"keyValueExtractionOperatorEnabled"`
	KeyValueExtractionConfig           ParseKeyValueConfig       `json:"keyValueExtractionConfig"`
	CsvExtractionOperatorEnabled       bool                      `json:"csvExtractionOperatorEnabled"`
	CsvExtractionConfig                ParseCsvConfig            `json:"csvExtractionConfig"`

	ConstantReplaceOperatorEnabled bool                  `json:"constantReplaceOperatorEnabled"`
	ConstantReplaceConfig          ConstantReplaceConfig `json:"constantConfig"`
	LowercaseOperatorEnabled       bool                  `json:"lowercaseOperatorEnabled"`
	UppercaseOperatorEnabled       bool                  `json:"uppercaseOperatorEnabled"`
	SplitOperatorEnabled           bool                  `json:"splitOperatorEnabled"`
	SplitOperatorConfig            SplitOperatorConfig   `json:"splitOperatorConfig"`
	// ✅ NEW: Math Function Operators
	MathAbsOperatorEnabled         bool                  `json:"mathAbsOperatorEnabled"`
	MathFloorOperatorEnabled       bool                  `json:"mathFloorOperatorEnabled"`
	MathCeilOperatorEnabled        bool                  `json:"mathCeilOperatorEnabled"`
	MathRoundOperatorEnabled       bool                  `json:"mathRoundOperatorEnabled"`
	MathModOperatorEnabled         bool                  `json:"mathModOperatorEnabled"`
	MathModOperatorConfig          MathModOperatorConfig `json:"mathModOperatorConfig,omitempty"`
	MathRandomIntOperatorEnabled   bool                  `json:"mathRandomIntOperatorEnabled"`
	MathRandomIntConfig            MathRandomIntConfig   `json:"mathRandomIntConfig,omitempty"`
	MathRandomBytesOperatorEnabled bool                  `json:"mathRandomBytesOperatorEnabled"`

	// ✅ NEW: New String Operators
	StringCamelCaseOperatorEnabled         bool                         `json:"stringCamelCaseOperatorEnabled"`
	StringRedactOperatorEnabled            bool                         `json:"stringRedactOperatorEnabled"`
	StringRedactOperatorConfig             StringRedactOperatorConfig   `json:"stringRedactOperatorConfig"`
	StringSliceOperatorEnabled             bool                         `json:"stringSliceOperatorEnabled"`
	StringSliceOperatorConfig              StringSliceOperatorConfig    `json:"stringSliceOperatorConfig"`
	StringTruncateOperatorEnabled          bool                         `json:"stringTruncateOperatorEnabled"`
	StringTruncateOperatorConfig           StringTruncateOperatorConfig `json:"stringTruncateOperatorConfig"`
	StringBase64EncodeOperatorEnabled      bool                         `json:"stringBase64EncodeOperatorEnabled"`
	StringBase64DecodeOperatorEnabled      bool                         `json:"stringBase64DecodeOperatorEnabled"`
	StringPercentEncodeOperatorEnabled     bool                         `json:"stringPercentEncodeOperatorEnabled"`
	StringPercentDecodeOperatorEnabled     bool                         `json:"stringPercentDecodeOperatorEnabled"`
	StringMD5OperatorEnabled               bool                         `json:"stringMD5OperatorEnabled"`
	StringSeaHashOperatorEnabled           bool                         `json:"stringSeaHashOperatorEnabled"`
	StringUUIDv4OperatorEnabled            bool                         `json:"stringUUIDv4OperatorEnabled"`
	StringEncodeASCIIOnlyOperatorEnabled   bool                         `json:"stringEncodeASCIIOnlyOperatorEnabled"`
	StringDecodeASCIIOnlyOperatorEnabled   bool                         `json:"stringDecodeASCIIOnlyOperatorEnabled"`
	StringReplaceWithLengthOperatorEnabled bool                         `json:"stringReplaceWithLengthOperatorEnabled"`
	// ✅ NEW: New Time Operators
	TimeFromUnixTimestampSecondsOperatorEnabled      bool `json:"timeFromUnixTimestampSecondsOperatorEnabled"`
	TimeFromUnixTimestampMillisecondsOperatorEnabled bool `json:"timeFromUnixTimestampMillisecondsOperatorEnabled"`
	TimeUnixTsSec                                    bool `json:"timeUnixTsSec"`
	TimeUnixTsMs                                     bool `json:"timeUnixTsMs"`

	//  ✅ NEW: Case Operators
	CaseOperatorEnabled   bool                 `json:"caseOperatorEnabled"`
	CaseOperatorConfig    CaseOperatorConfig   `json:"caseOperatorConfig"`
	NullIfOperatorEnabled bool                 `json:"nullIfOperatorEnabled"`
	NullIfOperatorConfig  NullIfOperatorConfig `json:"nullIfOperatorConfig"`

	//  ✅ NEW: to_string & to_int
	IntegerifyOperatorEnabled bool `json:"integerifyOperatorEnabled"`
	StringifyOperatorEnabled  bool `json:"stringifyOperatorEnabled"`
}

type RenameFieldsWithOperators struct {
	LogAttribute      string `json:"logAttribute"`
	DatabahnAttribute string `json:"databahnAttribute"`
	RenameAttribute   string `json:"renameAttribute"`
	Include           bool   `json:"include"`

	Operators []TransformationOperator `json:"operators"`
}
type CaseOperatorConfig struct {
	IfConditionKey string      `json:"ifConditionKey"`
	Conditions     []Condition `json:"conditions"`
	ElseValue      string      `json:"elseValue"`
}

type NullIfOperatorConfig struct {
	IfConditionKey string      `json:"ifConditionKey"`
	Conditions     []Condition `json:"conditions"`
}

type Condition struct {
	When     string `json:"when"`
	Then     string `json:"then"`
	Operator string `json:"operator"`
}
type MathRandomIntConfig struct {
	Min int `json:"min"`
	Max int `json:"max"`
}
type StringRedactOperatorConfig struct {
	Filters []string `json:"filters"`
}

type StringSliceOperatorConfig struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type StringTruncateOperatorConfig struct {
	Length int    `json:"length"`
	Suffix string `json:"suffix"`
}
type MathModOperatorConfig struct {
	Divisor int `json:"divisor"`
}
type SplitOperatorConfig struct {
	Delimiter string `json:"delimiter"`
	Index     int    `json:"index"`
}
type ConstantReplaceConfig struct {
	ConstantValue string `json:"constantValue"`
}
type ReplaceConfig struct {
	ReplacePattern string `json:"replacePattern"`
	ReplaceWith    string `json:"replaceWith"`
}
type MaskConfig struct {
	MaskPattern string `json:"maskPattern"`
	MaskWith    string `json:"maskWith"`
}
type SubstringExtractionConfig struct {
	ExtractionPattern string `json:"extractionPattern"`
}

type ExtractionConfig struct {
	DefaultValue  string `json:"defaultValue"`
	ExtractionKey string `json:"extractionKey"`
}

type ParseKeyValueConfig struct {
	Key               string `json:"key"`
	KeyValueDelimiter string `json:"keyValueDelimiter"`
	FieldDelimiter    string `json:"fieldDelimiter"`
}

type ParseCsvConfig struct {
	Index int `json:"index"`
}

type JsonExtractionConfig ExtractionConfig

type XmlExtractionConfig ExtractionConfig

type TimeNowConfig struct {
	Format       TimeFormats `json:"format"`
	CustomFormat string      `json:"customFormat,omitempty"`
}

type TimeFormats string

const (
	EpochMilliseconds TimeFormats = "EPOCH_MILLISECONDS"
	EpochSeconds      TimeFormats = "EPOCH_SECONDS"
	ISO8601           TimeFormats = "ISO8601"
	RFC3339           TimeFormats = "RFC3339"
	Custom            TimeFormats = "CUSTOM"
)

type ParseTimestampConfig struct {
	CustomFormat string `json:"customFormat,omitempty"`
}

type StringConcatConfig struct {
	Values    []ConcatValues `json:"values"`
	Delimiter string         `json:"delimiter,omitempty"`
}

type ConcatValues struct {
	FieldName string    `json:"fieldName"`
	FieldType FieldType `json:"fieldType"`
}

type FieldType string

const (
	Constant FieldType = "CONSTANT"
	Schema   FieldType = "SCHEMA"
)

type StartsWithConfig struct {
	Prefix        string `json:"prefix"`
	CaseSensitive bool   `json:"caseSensitive"`
}

type JoinStringArrayConfig struct {
	Delimiter string `json:"delimiter"`
}

type JoinObjectArrayKVPairsConfig struct {
	KeyField          string `json:"keyField"`
	ValueField        string `json:"valueField"`
	KeyValueDelimiter string `json:"keyValueDelimiter"`
	FieldDelimiter    string `json:"fieldDelimiter"`
}

type AdvancedCaseOperatorConfig struct {
	Conditions       []CaseCondition `json:"cases"`
	DefaultAction    Action          `json:"defaultAction"`
	ReferencedFields []string        `json:"referencedFields"`
}

type CaseCondition struct {
	Conditions            *TransformerRuleGroup `json:"conditions"`
	Action                Action                `json:"action"`
	GeneratedVrlCondition string                `json:"generatedVrlCondition"`
}

type TransformerRuleGroup struct {
	Rules      []TransformerRule `json:"rules"`
	Combinator string            `json:"combinator"` // "and", "or"

	// For simple conditions (when it's not a group but a single condition)
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type TransformerRule struct {
	Field      string                `json:"field"`
	Operator   string                `json:"operator"` // "=", "!=", ">", "<", ">=", "<=", "contains", "starts_with", "ends_with"
	Value      string                `json:"value"`
	Conditions *TransformerRuleGroup `json:"conditions"` // For nested conditions
}

type Action struct {
	Type                          ActionType                     `json:"type"`
	ConstantValueAssignmentConfig *ConstantValueAssignmentConfig `json:"constantValueAssignmentConfig,omitempty"`
	FieldValueAssignmentConfig    *FieldValueAssignmentConfig    `json:"fieldValueAssignmentConfig,omitempty"`
}

type ConstantValueAssignmentConfig struct {
	Value string `json:"value"`
}

type FieldValueAssignmentConfig struct {
	Field string `json:"field"`
}

type ActionType string

const (
	ConstantValueAssignment ActionType = "CONSTANT_VALUE_ASSIGNMENT"
	FieldValueAssignment    ActionType = "FIELD_VALUE_ASSIGNMENT"
)

type OcsfTransformationBlock struct {
	OcsfFilterCriteria OcsfFilterCriteria          `json:"ocsfFilterCriteria"`
	Name               string                      `json:"name"`
	OcsfCategory       string                      `json:"ocsfCategory"`
	OcsfClass          string                      `json:"ocsfClass"`
	Mappings           []RenameFieldsWithOperators `json:"mappings"`
	DerivedFields      []DerivedField              `json:"derivedFields"`
	Default            bool                        `json:"default"`
}
type OcsfFilterCriteria struct {
	Field      string               `json:"field"`
	Value      string               `json:"value"`
	Operator   string               `json:"operator"`
	Rules      []OcsfFilterCriteria `json:"rules"`
	Combinator string               `json:"combinator"`
}
