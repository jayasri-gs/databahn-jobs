package config

import (
	"context"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/databases"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"sync"
)

var appConfigLoader, secretsLoader, destinationConfigLoader sync.Once
var appConfigReader configuration.ConfigReader
var destinationConfigReader configuration.ConfigReader
var databaseConnection *databases.Connection
var openSearchCreds *configuration.OpenSearchCredentials

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
	secretName := appConfigReader.GetString(configuration.OpenSearchSecretName)
	region := appConfigReader.GetString(configuration.Region)
	creds, err := configuration.ReadOpenSearchSecrets(context.Background(), secretName, region)
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
	secretName := appConfigReader.GetString("database.secret_name")
	region := appConfigReader.GetString(configuration.Region)
	dbSecrets, err := databases.ReadDBSecrets(context.Background(), secretName, region)
	if err != nil {
		logger.GetLoggerWithContext(context.Background()).Error("error while fetching database credentials", zap.Error(err))
	}
	databaseConnection = &databases.Connection{
		Host:         appConfigReader.GetString(configuration.DatabaseHost),
		Port:         appConfigReader.GetString(configuration.DatabasePort),
		SchemaName:   appConfigReader.GetString(configuration.DatabaseSchema),
		DatabaseName: appConfigReader.GetString(configuration.DatabaseName),
		Credentials:  *dbSecrets,
	}
	dbConnection, err := databaseConnection.Connect(context.Background(), false)
	if err != nil {
		logger.GetLoggerWithContext(context.Background()).Error("error while connecting to database", zap.Error(err))
		return
	}
	db = dbConnection
}

func GetOpenSearchSecrets() configuration.OpenSearchCredentials {
	secretsLoader.Do(readOpenSearchConfigs)
	return *openSearchCreds
}
func GetDB() *gorm.DB {
	if db == nil {
		connectDB()
	}
	return db
}
