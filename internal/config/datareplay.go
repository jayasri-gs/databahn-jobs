package config

import (
	"github.com/databahn-ai/common-utils/utils"
)

type DataReplayConfig struct {
	dataMap map[string]string
}

func newDataReplayConfig() (*DataReplayConfig, error) {
	drc := &DataReplayConfig{
		dataMap: make(map[string]string),
	}
	drc.dataMap["urls.data_plane_controller_internal_base_url"] = utils.GetEnvOrDefault("DATA_PLANE_CONTROLLER_BASE_URL", "")
	return drc, nil
}

// DataReplayConfig implements the Config interface
// below func
func (drc *DataReplayConfig) GetString(key string) string {
	return drc.dataMap[key]
}

func (drc *DataReplayConfig) GetStringOrDefault(key string, defaultVal string) string {
	if val, ok := drc.dataMap[key]; ok {
		return val
	}
	return defaultVal
}

func (drc *DataReplayConfig) GetBool(key string) bool {
	return false
}

func (drc *DataReplayConfig) GetBoolOrDefault(key string, defaultVal bool) bool {
	return defaultVal
}

func (drc *DataReplayConfig) GetInt(key string) int {
	return 0
}

func (drc *DataReplayConfig) GetIntOrDefault(key string, defaultVal int) int {
	return defaultVal
}

func (drc *DataReplayConfig) GetStringMap(key string) map[string]any {
	return nil
}
func (drc *DataReplayConfig) GetStringMapString(key string) map[string]string {
	return nil
}
