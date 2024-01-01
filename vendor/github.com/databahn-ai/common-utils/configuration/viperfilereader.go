package configuration

import "github.com/spf13/viper"

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
	vw := ViperWrapper{vpr: config}
	return &vw, err
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
func (vw *ViperWrapper) GetInt(key string) int {
	return vw.vpr.GetInt(key)
}
func (vw *ViperWrapper) GetStringMap(key string) map[string]interface{} {
	return vw.vpr.GetStringMap(key)
}
func (vw *ViperWrapper) GetStringMapString(key string) map[string]string {
	return vw.vpr.GetStringMapString(key)
}
