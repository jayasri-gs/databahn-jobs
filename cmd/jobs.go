package cmd

import (
	"context"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/utils"
	ack "github.com/databahn-ai/databahn-jobs/internal/acknowledgement"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/datahealthscore"
	evntjobCmd "github.com/databahn-ai/databahn-jobs/internal/eventsequencing/jobcmd"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/jobs"
	"github.com/databahn-ai/databahn-jobs/internal/insights"
	"github.com/databahn-ai/databahn-jobs/internal/kafkaquery"
	"github.com/databahn-ai/databahn-jobs/internal/replay/jobcmd"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/stats"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
)

func RunJob(ctx context.Context, jobName string, input model.Message) {
	var err error
	switch jobName {
	case common.INSIGHTS_AGGREGATION:
		parallelism := utils.GetEnvInt("INSIGHTS_PROCESSING_PARALLELISM", 4)
		err = insights.AggregateInsightsAndStore(ctx, parallelism)
	case common.ROLLOVER_OLDER_STATS:
		err = stats.RolloverOlderStats(ctx)
	case common.STATS_LIFECYCLE:
		err = stats.RolloverLifecycle(ctx)
	case common.DEVICE_INVENTORY_HEALTH:
		runFor := utils.GetEnvOrDefault("DEVICE_INVENTORY_HEALTH_RUN_FOR", insights.HEALTH_CALCULATION_YESTERDAY)
		statuses, err2 := insights.CalculateDeviceInventoryHealth(ctx, runFor)
		err = err2
		logger.GetLogger().Info("device inventory health calculation completed", zap.String("runFor", runFor), zap.Any("statuses", statuses))
	case common.DATA_REPLAY:
		logger.GetLogger().Info("Data Replay Job Triggered")
		jobcmd.ExecuteReplayJob(input)
		logger.GetLogger().Info("Data Replay Job Completed")
	case common.FLEET_HEALTH_CHECKER:
		err = jobs.HealthCheckAlertForFleetNode(ctx)
	case common.LOG_SOURCE_ACTIVITY_CHECKER:
		err = jobs.AlertForLogSourceInactivity(ctx)
		err = jobs.AlertForDestinationInactivity(ctx)
	case common.LOG_SOURCE_REPUTATION_CHECKER:
		err = jobs.UpdateReputationForLogSources(ctx)
	case common.AGENT_HEALTH_CHECKER:
		err = jobs.AgentAlertForFleetNode(ctx)
	case common.TENANT_DAILY_DIGEST:
		err = jobs.TenantDailyDigest(ctx)
	case common.UNPARSED_EVENTS:
		err = jobs.AlertForUnparsedEvents(ctx)
	case common.KAFKA_QUERY:
		threadCount := utils.GetEnvInt("KAFKA_QUERY_THREAD_COUNT", 4)
		waitMinutes := utils.GetEnvInt("KAFKA_QUERY_WAIT_MINUTES", 5)
		brokers := config.GetAppConfiguration().GetString(configuration.KafkaBootstrapServers)
		query := utils.GetEnvOrDefault("KAFKA_QUERY_QUERY", "{}")
		kafkaquery.Start(ctx, brokers, query, threadCount, waitMinutes)
	case common.ACK_PROCESSOR:
		err = ack.ProcessAck()
	case common.EVENT_SEQUENCING:
		evntjobCmd.ExecuteS3DataSequencing(input)
	case common.ALERT_REPORT_PROCESSOR:
		err = auditReport.GenerateAuditReport(ctx)
	case common.DATA_HEALTH_SCORE_JOB:
		err = datahealthscore.CalculateDataHealthScore(ctx)
	case common.ENTITY_CHECKER_ALERT_GEN_V2:
		err = jobs.UpdateLastEventTime(ctx)
	case common.SILENT_DEVICE_ALERT:
		err = jobs.ProcessSilentDevices(ctx)
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
