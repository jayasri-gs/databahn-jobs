package alerts_async

import "strings"

type Functionality interface {
	String() string
	privateFunctionality()
}

type functionalityEnum struct {
	value string
}

func (f functionalityEnum) String() string {
	return f.value
}

func (f functionalityEnum) privateFunctionality() {
}

var (
	Agent             = functionalityEnum{value: "agent"}
	LogSource         = functionalityEnum{value: "log_source"}
	CloudLogSource    = functionalityEnum{value: "cloud_log_source"}
	Fleet             = functionalityEnum{value: "fleet"}
	FleetNode         = functionalityEnum{value: "fleet_node"}
	FleetConnector    = functionalityEnum{value: "fleet_connector"}
	FleetComponent    = functionalityEnum{value: "fleet_component"}
	Dispenser         = functionalityEnum{value: "dispenser"}
	DataVolume        = functionalityEnum{value: "data_volume"}
	VolumeControlRule = functionalityEnum{value: "volume_control_rule"}
	Lookup            = functionalityEnum{value: "lookup"}
	Enrichment        = functionalityEnum{value: "enrichment"}
	Transformer       = functionalityEnum{value: "transformation"}
	RouteProcessor    = functionalityEnum{value: "route_processor"}
	InsightsRule      = functionalityEnum{value: "insights_rule"}
	GlobalDestination = functionalityEnum{value: "global_destination"}
	AuditReport       = functionalityEnum{value: "audit_report"}
	Unknown           = functionalityEnum{value: "unknown"}
)

func GetAllFunctionalities() []Functionality {
	return []Functionality{
		Agent,
		LogSource,
		CloudLogSource,
		Fleet,
		FleetConnector,
		FleetComponent,
		FleetNode,
		Dispenser,
		VolumeControlRule,
		Lookup,
		Enrichment,
		Transformer,
		RouteProcessor,
		InsightsRule,
		GlobalDestination,
		Unknown,
	}
}

func ParseFunctionality(functionality string) Functionality {
	for _, m := range GetAllFunctionalities() {
		if strings.EqualFold(m.String(), functionality) {
			return m
		}
	}
	return Unknown
}
