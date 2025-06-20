package alerts_async

type AlertType interface {
	String() string
	privateAlertType()
}

type alertTypeEnum struct {
	value string
}

func (f alertTypeEnum) String() string {
	return f.value
}

func (f alertTypeEnum) privateAlertType() {
}

var (
	Internal = alertTypeEnum{value: "internal"}
	External = alertTypeEnum{value: "external"}
)
