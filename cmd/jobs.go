package cmd

import (
	"context"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/insights"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
)

func RunJob(ctx context.Context, jobName string) {
	var err error
	switch jobName {
	case common.INSIGHTS_AGGREGATION:
		parallelism := utils.GetEnvInt("INSIGHTS_PROCESSING_PARALLELISM", 4)
		err = insights.AggregateInsightsAndStore(ctx, parallelism)
	case common.DEVICE_INVENTORY_HEALTH:
		runFor := utils.GetEnvOrDefault("DEVICE_INVENTORY_HEALTH_RUN_FOR", insights.HEALTH_CALCULATION_YESTERDAY)
		statuses, err2 := insights.CalculateDeviceInventoryHealth(ctx, runFor)
		err = err2
		logger.GetLogger().Info("device inventory health calculation completed", zap.String("runFor", runFor), zap.Any("statuses", statuses))
	default:
		logger.GetLogger().Panic("unknown job", zap.String("jobName", jobName))
	}
	if err != nil {
		logger.GetLogger().Error("failed to process job", zap.Error(err), zap.String("jobName", jobName))
		logger.GetLogger().Sync()
		os.Exit(1)
	} else {
		logger.GetLogger().Info("successfully processed job", zap.String("jobName", jobName))
		logger.GetLogger().Sync()
		os.Exit(0)
	}
}
