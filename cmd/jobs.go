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
