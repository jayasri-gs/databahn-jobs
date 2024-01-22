package common

import (
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"sync"
)

var appConfigLoader sync.Once
var appConfigReader configuration.ConfigReader

func loadAppConfigReader() {
	conf, err := configuration.NewAppConfig()
	if err != nil {
		logger.GetLogger().Panic("failed to read app configuration", zap.Error(err))
	}
	appConfigReader = conf
}

func GetAppConfiguration() configuration.ConfigReader {
	appConfigLoader.Do(loadAppConfigReader)
	return appConfigReader
}
