package alerts_async

type Status interface {
	Value() int
	privateStatus()
}

type statusEnum struct {
	value int
}

func (f statusEnum) Value() int {
	return f.value
}

func (f statusEnum) privateStatus() {
}

var (
	AlertOpen         = statusEnum{1}
	AlertDismissed    = statusEnum{2}
	AlertResolved     = statusEnum{3}
	AlertAutoResolved = statusEnum{4}
)
