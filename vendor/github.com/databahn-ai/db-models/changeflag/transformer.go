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
	Integerify                        TransformationOpType = "INTEGERIFY"
	Stringify                         TransformationOpType = "STRINGIFY"
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
	RenameFields []RenameFieldsWithOperators `json:"renameFields"`
}
type TrimConfig struct {
	StripLeading  bool `json:"stripLeading"`
	StripTrailing bool `json:"stripTrailing"`
}
type TransformationOperator struct {
	Type TransformationOpType `json:"type"`

	ReplaceConfig                *ReplaceConfig                `json:"replaceConfig,omitempty"`
	TrimConfig                   *TrimConfig                   `json:"trimConfig,omitempty"`
	MaskConfig                   *MaskConfig                   `json:"maskConfig,omitempty"`
	SubstringExtractionConfig    *SubstringExtractionConfig    `json:"substringExtractionConfig,omitempty"`
	ConstantReplaceConfig        *ConstantReplaceConfig        `json:"constantReplaceConfig,omitempty"`
	SplitOperatorConfig          *SplitOperatorConfig          `json:"splitOperatorConfig,omitempty"`
	StringRedactOperatorConfig   *StringRedactOperatorConfig   `json:"stringRedactOperatorConfig,omitempty"`
	StringTruncateOperatorConfig *StringTruncateOperatorConfig `json:"stringTruncateOperatorConfig,omitempty"`
	CaseOperatorConfig           *CaseOperatorConfig           `json:"caseOperatorConfig,omitempty"`
}

type RenameFields struct {
	LogAttribute                       string                    `json:"logAttribute"`
	DatabahnAttribute                  string                    `json:"databahnAttribute"`
	RenameAttribute                    string                    `json:"renameAttribute"`
	Include                            bool                      `json:"include"`
	ReplaceOperatorEnabled             bool                      `json:"replaceOperatorEnabled"`
	ReplaceConfig                      ReplaceConfig             `json:"replaceConfig"`
	TrimOperatorEnabled                bool                      `json:"trimOperatorEnabled"`
	NumerifyOperatorEnabled            bool                      `json:"numerifyOperatorEnabled"`
	MaskOperatorEnabled                bool                      `json:"maskOperatorEnabled"`
	MaskConfig                         MaskConfig                `json:"maskConfig"`
	SubstringExtractionOperatorEnabled bool                      `json:"substringExtractionOperatorEnabled"`
	SubstringExtractionConfig          SubstringExtractionConfig `json:"substringExtractionConfig"`
	ConstantReplaceOperatorEnabled     bool                      `json:"constantReplaceOperatorEnabled"`
	ConstantReplaceConfig              ConstantReplaceConfig     `json:"constantConfig"`
	LowercaseOperatorEnabled           bool                      `json:"lowercaseOperatorEnabled"`
	UppercaseOperatorEnabled           bool                      `json:"uppercaseOperatorEnabled"`
	SplitOperatorEnabled               bool                      `json:"splitOperatorEnabled"`
	SplitOperatorConfig                SplitOperatorConfig       `json:"splitOperatorConfig"`
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
	CaseOperatorEnabled bool               `json:"caseOperatorEnabled"`
	CaseOperatorConfig  CaseOperatorConfig `json:"caseOperatorConfig"`

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
type OcsfTransformationBlock struct {
	OcsfFilterCriteria OcsfFilterCriteria          `json:"ocsfFilterCriteria"`
	Name               string                      `json:"name"`
	OcsfCategory       string                      `json:"ocsfCategory"`
	OcsfClass          string                      `json:"ocsfClass"`
	Mappings           []RenameFieldsWithOperators `json:"mappings"`
	Default            bool                        `json:"default"`
}
type OcsfFilterCriteria struct {
	Field      string               `json:"field"`
	Value      string               `json:"value"`
	Operator   string               `json:"operator"`
	Rules      []OcsfFilterCriteria `json:"rules"`
	Combinator string               `json:"combinator"`
}
