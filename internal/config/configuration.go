package config

import (
	"context"
	"sync"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/databases"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var appConfigLoader, secretsLoader, destinationConfigLoader, alertConfigLoader sync.Once
var appConfigReader configuration.ConfigReader
var destinationConfigReader configuration.ConfigReader
var databaseConnection *databases.Connection
var openSearchCreds *configuration.OpenSearchCredentials
var alertConfigReader configuration.ConfigReader

var db *gorm.DB

func loadAppConfigReader() {
	conf, err := configuration.NewAppConfig()
	if err != nil {
		logger.GetLogger().Panic("failed to read app configuration", zap.Error(err))
	}
	appConfigReader = conf
}

func loadDestinationConfigReader() {

	confDestination, err := configuration.NewConfig(configuration.DestinationConfig)
	if err != nil {
		logger.GetLogger().Panic("failed to read Destination configuration", zap.Error(err))
	}
	destinationConfigReader = confDestination
}

func readOpenSearchConfigs() {
	creds, err := configuration.ReadOpenSearchSecrets(context.Background(), appConfigReader)
	if err != nil {
		logger.GetLogger().Error("error while reading opensearch secret", zap.Error(err))
	}
	openSearchCreds = creds
}

func GetAppConfiguration() configuration.ConfigReader {
	appConfigLoader.Do(loadAppConfigReader)
	return appConfigReader
}

func GetDestinationConfiguration() configuration.ConfigReader {
	destinationConfigLoader.Do(loadDestinationConfigReader)
	return destinationConfigReader
}

func connectDB() {
	databaseConnection = &databases.Connection{
		Host:         appConfigReader.GetString(configuration.DatabaseHost),
		Port:         appConfigReader.GetString(configuration.DatabasePort),
		SchemaName:   appConfigReader.GetString(configuration.DatabaseSchema),
		DatabaseName: appConfigReader.GetString(configuration.DatabaseName),
	}
	dbConnection, err := databaseConnection.ConnectWithSecrets(context.Background(), false, appConfigReader)
	if err != nil {
		logger.GetLoggerWithContext(context.Background()).Panic("error while connecting to database", zap.Error(err))
		return
	}
	db = dbConnection
}

func GetDB() *gorm.DB {
	if db == nil {
		connectDB()
	}
	return db
}

func GetAlertConfiguration() configuration.ConfigReader {
	alertConfigLoader.Do(loadAlertConfigReader)
	return alertConfigReader
}
func loadAlertConfigReader() {
	conf, err := configuration.NewConfig("alert")
	if err != nil {
		logger.GetLogger().Panic("failed to read alert configuration", zap.Error(err))
	}
	alertConfigReader = conf
}

func GetDataReplayConfiguration() configuration.ConfigReader {
	appConfigLoader.Do(loadDataReplayConfigReader)
	return appConfigReader
}

func loadDataReplayConfigReader() {
	conf, err := newDataReplayConfig()
	if err != nil {
		logger.GetLogger().Panic("failed to read data replay configuration", zap.Error(err))
	}
	appConfigReader = conf
}
