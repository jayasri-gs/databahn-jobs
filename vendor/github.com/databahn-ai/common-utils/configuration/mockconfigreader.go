package configuration

type MockConfigReader struct {
	mp map[string]any
}

func NewMockConfigReader(mp map[string]any) *MockConfigReader {
	return &MockConfigReader{mp: mp}
}

func (mcr *MockConfigReader) GetString(key string) string {
	val := mcr.mp[key]
	if v, ok := val.(string); ok {
		return v
	} else {
		return ""
	}
}
func (mcr *MockConfigReader) GetStringOrDefault(key string, def string) string {
	val := mcr.mp[key]
	if v, ok := val.(string); ok {
		return v
	} else {
		return def
	}
}
func (mcr *MockConfigReader) GetBool(key string) bool {
	val := mcr.mp[key]
	if v, ok := val.(bool); ok {
		return v
	} else {
		return false
	}
}
func (mcr *MockConfigReader) GetInt(key string) int {
	val := mcr.mp[key]
	if v, ok := val.(int); ok {
		return v
	} else {
		return 0
	}
}
func (mcr *MockConfigReader) GetStringMap(key string) map[string]any {
	val := mcr.mp[key]
	if v, ok := val.(map[string]interface{}); ok {
		return v
	} else {
		return map[string]any{}
	}
}

func (mcr *MockConfigReader) GetStringMapString(key string) map[string]string {
	val := mcr.mp[key]
	if v, ok := val.(map[string]string); ok {
		return v
	} else {
		return map[string]string{}
	}
}
