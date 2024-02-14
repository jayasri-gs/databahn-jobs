package database

import (
	"context"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/databases"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	database *gorm.DB
	dbReady  = false
)

// DBInit initializes connection to database
func init() {
	conf, err := configuration.NewConfigReader()
	if err != nil {
		logger.GetLogger().Error("Unable read config", zap.Error(err))
		return
	}
	db, err := databases.Initialize(conf)
	if err != nil {
		logger.GetLogger().Error("Unable to connect to database", zap.Error(err))
		return
	}
	database = db
	dbReady = true
}

// GetDB returns an instance of db for doing db stuff(s).
func GetDB() *gorm.DB {
	return database
}

func AutoMigrate(ctx context.Context, dataObject interface{}) error {
	return database.WithContext(ctx).AutoMigrate(&dataObject)
}

func IsDBReady() bool {
	return dbReady
}
