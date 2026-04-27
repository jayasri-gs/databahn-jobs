package jobs

import (
	"encoding/json"
	"math"
	"time"
)

func aggKeyString(m map[string]any, key string) (string, bool) {
	if m == nil {
		return "", false
	}
	v, ok := m[key]
	if !ok {
		return "", false
	}
	return stringFromAny(v)
}

func aggValueFloat64(m map[string]any, key string) (float64, bool) {
	if m == nil {
		return 0, false
	}
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	return float64FromAny(v)
}

func stringFromAny(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	default:
		return "", false
	}
}

func float64FromAny(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// unixMilliUTCFromFloat64 converts max-aggregation millis from OpenSearch to UTC.
func unixMilliUTCFromFloat64(ms float64) (time.Time, bool) {
	if math.IsNaN(ms) || math.IsInf(ms, 0) || ms < 0 {
		return time.Time{}, false
	}
	if ms > 1e15 {
		return time.Time{}, false
	}
	return time.UnixMilli(int64(ms)).UTC(), true
}
