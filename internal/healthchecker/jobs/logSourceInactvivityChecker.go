package jobs

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"time"
)

func SendAlertsForInactivity(ctx context.Context) error {
	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Info("Failed to get tenants")
		return err
	}

	alertConfig, err := helper.CreateEntityAlertsConfigMapByTenant(db)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Info("Failed to load alert config")
	}

	for _, t := range tenants {
		now := time.Now().UTC()

		for interval, conf := range alertConfig {

			logger.GetLoggerWithContext(ctx).Info("", zap.Any("config", conf), zap.String("interval", interval), zap.String("tenant", t.Id.String()), zap.Any("now", now))
		}
	}
	return nil

}
