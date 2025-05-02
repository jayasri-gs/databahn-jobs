package rule

import (
	"errors"
	"time"
)

type SuppressionConfig struct {
	Attributes []string            `json:"attributes"`
	Interval   SuppressionInterval `json:"interval"`
	MaxKeys    int64               `json:"maxKeys"`
	EventCount int64               `json:"eventCount"`
}

type SuppressionInterval struct {
	Value int    `json:"value"`
	Unit  string `json:"unit"`
}

func (d SuppressionInterval) GetDuration() (time.Duration, error) {
	switch d.Unit {
	case MINUTE_INTERVAL:
		return time.Duration(d.Value) * time.Minute, nil
	case HOUR_INTERVAL:
		return time.Duration(d.Value) * time.Hour, nil
	case DAY_INTERVAL:
		return time.Duration(d.Value) * time.Hour, nil
	}
	return time.Duration(0), errors.New("unsupported interval unit " + d.Unit)
}
