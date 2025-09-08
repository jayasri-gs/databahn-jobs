package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/jobs/vc"
	"github.com/databahn-ai/databahn-jobs/internal/transformationCheckerUtility"
	"github.com/databahn-ai/db-models/alerts_async"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/utils"
	ack "github.com/databahn-ai/databahn-jobs/internal/acknowledgement"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	cp_jobs "github.com/databahn-ai/databahn-jobs/internal/cp_alerts/jobs"
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
		err = jobs.SendAlertsForActivity(ctx)
		err = jobs.AlertForDestinationInactivity(ctx)
	case common.LOG_SOURCE_ACTIVITY_CHECKER_NEW:
		err = cp_jobs.AlertForNoEventsFromSources(ctx)
	case common.DESTINATION_ACTIVITY_CHECKER:
		err = cp_jobs.AlertForNoEventsToDestination(ctx)
	case common.DESTINATION_MORE_THAN_INJECTED:
		err = cp_jobs.AlertDestinationsWithMoreDataDeliveredThanInjection(ctx)
	case common.NOTIFICATIONS_FOR_ALERTS:
		err = cp_jobs.SendNotificationsForAlerts(ctx)
	case common.HEALTH_CHECKER:
		err = cp_jobs.HealthCheckJob(ctx)
	case common.TENANT_DAILY_DIGEST:
		err = cp_jobs.SendTenantDailyDigest(ctx)
	case common.UNPARSED_EVENTS:
		err = cp_jobs.SendAlertsForUnparsedEvents(ctx)
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
	case common.FHL_WINDOWS_ACTIVITY_CHECKER:
		err = jobs.CheckAndRestartFHLAgent(ctx)
	case common.DEVICE_INVENTORY_ALERT:
		err = cp_jobs.SendAlertForDeviceLevelAlert(ctx)
	case common.VOLUME_DEVIATION_ALERT:
		err = cp_jobs.SendAlertForVolumeDeviation(ctx)
	case common.VC_NO_REDUCTION_ALERT:
		err = cp_jobs.SendAlertForVCNoReduction(ctx)
	case common.DEPLOYMENT_DELAY_ALERT:
		err = cp_jobs.SendAlertForDeploymentDelayAlert(ctx)
	case common.VC_ALERTS:
		err = vc.SendAlertsForVCRules(ctx)
	case common.TRANSFORMATION_VALIDATOR:
		err = transformationCheckerUtility.ValidateTransformations(ctx)
	default:
		logger.GetLogger().Panic("unknown job", zap.String("jobName", jobName))
	}
	if err != nil {
		sendJobFailureAlert(ctx, jobName, input, err)
		logger.GetLogger().Error("failed to process job", zap.Error(err), zap.String("jobName", jobName))
		logger.GetLogger().Sync()
		os.Exit(1)
	} else {
		logger.GetLogger().Info("successfully processed job", zap.String("jobName", jobName))
		logger.GetLogger().Sync()
		os.Exit(0)
	}
}

// sendJobFailureAlert sends an engineering alert when a job fails
func sendJobFailureAlert(ctx context.Context, jobName string, input model.Message, err error) {
	// Initialize alerts manager
	alertsManager, alertErr := alert.NewAlertsManager(ctx)
	if alertErr != nil {
		logger.GetLogger().Error("failed to create alerts manager for job failure alert", zap.Error(alertErr))
		return
	}
	defer alertsManager.Close(ctx)

	alert, alertErr := alerts_async.NewAlert(
		alerts_async.Job,
		alerts_async.WithTitle(fmt.Sprintf("Databahn Job Failure Alert (%v)", jobName)),
		alerts_async.WithMessage(formatJobFailureMessage(jobName, err, input)),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.JobFailure),
		alerts_async.WithEntityDetails("job", jobName, "default-dataplane", input.TenantId),
		alerts_async.WithErrorCode(alerts_async.DJFE10001, err.Error()),
	)

	if alertErr != nil {
		logger.GetLogger().Error("failed to create job failure alert", zap.Error(alertErr), zap.String("jobName", jobName))
		return
	}

	// Send the alert
	alertErr = alertsManager.SendAlerts([]*alerts_async.Alert{alert})
	if alertErr != nil {
		logger.GetLogger().Error("failed to send job failure alert", zap.Error(alertErr), zap.String("jobName", jobName))
		return
	}

	logger.GetLogger().Info("job failure alert sent successfully",
		zap.String("jobName", jobName),
		zap.String("severity", alerts_async.Critical.String()),
		zap.String("error", err.Error()))
}

// formatJobFailureMessage creates a detailed error message for the alert
func formatJobFailureMessage(jobName string, err error, input model.Message) string {
	message := "Job '" + jobName + "' failed with error: " + err.Error()

	// Add job parameters if available
	if input.RequestId != "" {
		message += ". Request ID: " + input.RequestId
	}
	if input.TenantId != "" {
		message += ". Tenant ID: " + input.TenantId
	}
	if input.Source != "" {
		message += ". Source: " + input.Source
	}

	return message
}
