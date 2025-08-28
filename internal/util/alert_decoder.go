package util

import (
	"reflect"
	"strconv"
	"time"

	"github.com/mitchellh/mapstructure"
)

// CreateAlertDecoder creates a mapstructure decoder with proper timestamp handling
// This function handles the conversion of string timestamps from OpenSearch to int64 fields
// in both alerts_async.Alert and statistics.AlertDocument structs
func CreateAlertDecoder(result interface{}) (*mapstructure.Decoder, error) {
	return mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName: "json",
		Result:  result,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
				if f.Kind() == reflect.String && t.Kind() == reflect.Int64 {
					// Handle timestamp string to int64 conversion
					if str, ok := data.(string); ok {
						if val, err := strconv.ParseInt(str, 10, 64); err == nil {
							return val, nil
						}

						// Try to parse as RFC3339 timestamp
						if t, err := time.Parse(time.RFC3339, str); err == nil {
							return t.UnixMilli(), nil
						}
						// Try to parse as RFC3339Nano timestamp
						if t, err := time.Parse(time.RFC3339Nano, str); err == nil {
							return t.UnixMilli(), nil
						}
						// Try to parse as custom format with milliseconds
						if t, err := time.Parse("2006-01-02T15:04:05.999Z07:00", str); err == nil {
							return t.UnixMilli(), nil
						}
						if t, err := time.Parse("2006-01-02T15:04:05.999-07:00", str); err == nil {
							return t.UnixMilli(), nil
						}
						// Try to parse as RFC3339 with milliseconds and Z timezone (UTC)
						if t, err := time.Parse("2006-01-02T15:04:05.999Z", str); err == nil {
							return t.UnixMilli(), nil
						}
						if t, err := time.Parse("2006-01-02T15:04:05.999", str); err == nil {
							return t.UnixMilli(), nil
						}
					}
				}
				return data, nil
			},
		),
	})
}
