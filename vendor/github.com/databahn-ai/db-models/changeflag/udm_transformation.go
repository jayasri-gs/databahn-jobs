package changeflag

// UdmTransformationMappings represents Chronicle UDM transformation configuration with multiple blocks.
type UdmTransformationMappings struct {
	UdmTransformationBlocks []UdmTransformationBlock `json:"udmTransformationBlocks"`
	GetFlattenedOutput      bool                     `json:"getFlattenedOutput"`
}

// UdmTransformationBlock represents a single UDM event type transformation configuration.
type UdmTransformationBlock struct {
	UdmFilterCriteria UdmFilterCriteria           `json:"udmFilterCriteria"`
	Category          string                      `json:"category"`
	EventType         string                      `json:"event_type"`
	Mappings          []RenameFieldsWithOperators `json:"mappings"`
	DerivedFields     []DerivedField              `json:"derivedFields"`
	Default           bool                        `json:"default"`
}

// UdmFilterCriteria defines conditional logic for routing events to specific UDM blocks.
type UdmFilterCriteria struct {
	Field      string              `json:"field"`
	Value      string              `json:"value"`
	Operator   string              `json:"operator"`
	Rules      []UdmFilterCriteria `json:"rules"`
	Combinator string              `json:"combinator"`
}
