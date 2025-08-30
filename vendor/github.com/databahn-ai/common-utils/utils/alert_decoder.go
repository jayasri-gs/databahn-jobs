package utils

import (
	"github.com/mitchellh/mapstructure"
	"reflect"
	"strconv"
	"time"
)

func CreateAlertDecoder(result interface{}) (*mapstructure.Decoder, error) {
	return mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName: "json",
		Result:  result,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
				if f.Kind() == reflect.String && t.Kind() == reflect.Int64 {
					// Handle timestamp string to int64 conversion
					if str, ok := data.(string); ok {
						// First try to parse as numeric string (your case)
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
