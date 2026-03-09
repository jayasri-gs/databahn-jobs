package util

import (
	"github.com/databahn-ai/common-utils/configuration"
)

// overlayConfigReader implements configuration.ConfigReader by delegating to a base
// config and overriding with a map of key -> value (string, bool, or int).
type overlayConfigReader struct {
	base      configuration.ConfigReader
	overrides map[string]interface{}
}

// GetString returns the override value if present, otherwise base.GetString(key).
func (o *overlayConfigReader) GetString(key string) string {
	if v, ok := o.overrides[key]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return o.base.GetString(key)
}

// GetStringOrDefault returns the override value if present, otherwise base.GetStringOrDefault(key, def).
func (o *overlayConfigReader) GetStringOrDefault(key string, def string) string {
	if v, ok := o.overrides[key]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return o.base.GetStringOrDefault(key, def)
}

// GetBool returns the override value if present, otherwise base.GetBool(key).
func (o *overlayConfigReader) GetBool(key string) bool {
	if v, ok := o.overrides[key]; ok && v != nil {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return o.base.GetBool(key)
}

// GetBoolOrDefault returns the override value if present, otherwise base.GetBoolOrDefault(key, def).
func (o *overlayConfigReader) GetBoolOrDefault(key string, def bool) bool {
	if v, ok := o.overrides[key]; ok && v != nil {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return o.base.GetBoolOrDefault(key, def)
}

// GetInt returns the override value if present, otherwise base.GetInt(key).
func (o *overlayConfigReader) GetInt(key string) int {
	if v, ok := o.overrides[key]; ok && v != nil {
		if i, ok := v.(int); ok {
			return i
		}
	}
	return o.base.GetInt(key)
}

// GetIntOrDefault returns the override value if present, otherwise base.GetIntOrDefault(key, def).
func (o *overlayConfigReader) GetIntOrDefault(key string, def int) int {
	if v, ok := o.overrides[key]; ok && v != nil {
		if i, ok := v.(int); ok {
			return i
		}
	}
	return o.base.GetIntOrDefault(key, def)
}

// GetStringMap delegates to base.
func (o *overlayConfigReader) GetStringMap(key string) map[string]any {
	return o.base.GetStringMap(key)
}

// GetStringMapString delegates to base.
func (o *overlayConfigReader) GetStringMapString(key string) map[string]string {
	return o.base.GetStringMapString(key)
}

// Ensure overlayConfigReader implements configuration.ConfigReader at compile time.
var _ configuration.ConfigReader = (*overlayConfigReader)(nil)
