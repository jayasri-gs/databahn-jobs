package configuration

import (
	"errors"

	"github.com/databahn-ai/common-utils/configs"
)

// ErrConfigKeyMissing is returned by GetBoolRequired when the key is not set.
var ErrConfigKeyMissing = errors.New("configuration key is not set")

type ConfigReader interface {
	GetString(string) string
	GetStringOrDefault(string, string) string
	GetBool(string) bool
	GetBoolOrDefault(string, bool) bool
	// GetBoolRequired returns the boolean value only if the key is set; otherwise it returns ErrConfigKeyMissing.
	GetBoolRequired(string) (bool, error)
	GetInt(string) int
	GetIntOrDefault(string, int) int
	GetStringMap(string) map[string]any
	GetStringMapString(string) map[string]string
}

// NewConfigReader method is wrapper over older aws parameter store based config reader
// Deprecated: please use NewAppConfig or NewConfig
func NewConfigReader() (ConfigReader, error) {
	config, _, err := configs.InitializeConfig()
	wp := ViperWrapper{vpr: config}
	return &wp, err
}

func NewAppConfig() (ConfigReader, error) {
	return NewConfig(AppConfigName)
}

func NewConfig(name string) (ConfigReader, error) {
	return newViperConfigFromFile(name)
}

func NewScaleConfig() (ConfigReader, error) {
	return newViperConfigFromFile(ScaleConfigName)
}
