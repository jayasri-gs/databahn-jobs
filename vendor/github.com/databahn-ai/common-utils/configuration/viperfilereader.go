package configuration

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
)

const basePath = "/opt/databahn/config"
const localPath = "$HOME/config"
const wd = "."

func newViperConfigFromFile(name string) (ConfigReader, error) {
	config := viper.New()
	config.AutomaticEnv()
	config.SetConfigName(name)
	config.SetConfigType("yaml")
	config.AddConfigPath(basePath)
	config.AddConfigPath(localPath)
	config.AddConfigPath(wd)
	err := config.ReadInConfig()
	return &ViperWrapper{vpr: config}, err
}

// NewScaleConfigOptional loads scale.yaml from the same paths as other file configs.
// If the file is missing, it returns a reader over an empty Viper (no error).
// Invalid YAML or other read errors are returned; callers that need strict loading should use NewScaleConfig.
func NewScaleConfigOptional() (ConfigReader, error) {
	w, err := newViperConfigFromFile(ScaleConfigName)
	if err == nil {
		return w, nil
	}
	var notFound viper.ConfigFileNotFoundError
	if errors.As(err, &notFound) {
		return w, nil
	}
	return nil, err
}

type ViperWrapper struct {
	vpr *viper.Viper
}

func (vw *ViperWrapper) GetString(key string) string {
	return vw.vpr.GetString(key)
}
func (vw *ViperWrapper) GetStringOrDefault(key string, def string) string {
	str := vw.vpr.GetString(key)
	if str == "" {
		return def
	}
	return str
}
func (vw *ViperWrapper) GetBool(key string) bool {
	return vw.vpr.GetBool(key)
}
func (vw *ViperWrapper) GetBoolOrDefault(key string, def bool) bool {
	if !vw.vpr.IsSet(key) {
		return def
	}
	return vw.vpr.GetBool(key)
}

func (vw *ViperWrapper) GetBoolRequired(key string) (bool, error) {
	if !vw.vpr.IsSet(key) {
		return false, fmt.Errorf("%w: %q", ErrConfigKeyMissing, key)
	}
	return vw.vpr.GetBool(key), nil
}

func (vw *ViperWrapper) GetInt(key string) int {
	return vw.vpr.GetInt(key)
}
func (vw *ViperWrapper) GetIntOrDefault(key string, def int) int {
	if !vw.vpr.IsSet(key) {
		return def
	}
	return vw.vpr.GetInt(key)
}
func (vw *ViperWrapper) GetStringMap(key string) map[string]interface{} {
	return vw.vpr.GetStringMap(key)
}
func (vw *ViperWrapper) GetStringMapString(key string) map[string]string {
	return vw.vpr.GetStringMapString(key)
}
