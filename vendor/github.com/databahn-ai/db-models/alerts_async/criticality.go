package alerts_async

type Criticality interface {
	String() string
	privateCriticality()
}

type criticalityEnum struct {
	value string
}

func (f criticalityEnum) String() string {
	return f.value
}

func (f criticalityEnum) privateCriticality() {
}

var (
	Info     = criticalityEnum{value: "info"}
	Warning  = criticalityEnum{value: "warning"}
	Sever    = criticalityEnum{value: "severe"}
	Critical = criticalityEnum{value: "critical"}
)
