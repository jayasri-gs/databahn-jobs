package alerts_async

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
	LogSource      = functionalityEnum{value: "log_source"}
	CloudLogSource = functionalityEnum{value: "cloud_log_source"}
	Fleet          = functionalityEnum{value: "fleet"}
	FleetNode      = functionalityEnum{value: "fleet_node"}
	Dispenser      = functionalityEnum{value: "dispenser"}
)
