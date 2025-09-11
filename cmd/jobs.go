package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/jobs/vc"
	"github.com/databahn-ai/databahn-jobs/internal/transformationCheckerUtility"
	"github.com/databahn-ai/databahn-jobs/internal/unparsedReport"
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
	var result common.JobResult

	switch jobName {
	case common.INSIGHTS_AGGREGATION:
		parallelism := utils.GetEnvInt("INSIGHTS_PROCESSING_PARALLELISM", 4)
		result = insights.AggregateInsightsAndStore(ctx, parallelism)
	case common.ROLLOVER_OLDER_STATS:
		result = stats.RolloverOlderStats(ctx)
	case common.STATS_LIFECYCLE:
		result = stats.RolloverLifecycle(ctx)
	case common.DEVICE_INVENTORY_HEALTH:
		runFor := utils.GetEnvOrDefault("DEVICE_INVENTORY_HEALTH_RUN_FOR", insights.HEALTH_CALCULATION_YESTERDAY)
		result = insights.CalculateDeviceInventoryHealth(ctx, runFor)
	case common.DATA_REPLAY:
		logger.GetLogger().Info("Data Replay Job Triggered")
		result = jobcmd.ExecuteReplayJob(input)
	case common.LOG_SOURCE_ACTIVITY_CHECKER_NEW:
		result = cp_jobs.AlertForNoEventsFromSources(ctx)
	case common.DESTINATION_ACTIVITY_CHECKER:
		result = cp_jobs.AlertForNoEventsToDestination(ctx)
	case common.DESTINATION_MORE_THAN_INJECTED:
		result = cp_jobs.AlertDestinationsWithMoreDataDeliveredThanInjection(ctx)
	case common.NOTIFICATIONS_FOR_ALERTS:
		result = cp_jobs.SendNotificationsForAlerts(ctx)
	case common.HEALTH_CHECKER:
		result = cp_jobs.HealthCheckJob(ctx)
	case common.TENANT_DAILY_DIGEST:
		result = cp_jobs.SendTenantDailyDigest(ctx)
	case common.UNPARSED_EVENTS:
		result = cp_jobs.SendAlertsForUnparsedEvents(ctx)
	case common.KAFKA_QUERY:
		threadCount := utils.GetEnvInt("KAFKA_QUERY_THREAD_COUNT", 4)
		waitMinutes := utils.GetEnvInt("KAFKA_QUERY_WAIT_MINUTES", 5)
		brokers := config.GetAppConfiguration().GetString(configuration.KafkaBootstrapServers)
		query := utils.GetEnvOrDefault("KAFKA_QUERY_QUERY", "{}")
		err := kafkaquery.Start(ctx, brokers, query, threadCount, waitMinutes)
		if err != nil {
			result = common.NewJobResultFromError(err)
		} else {
			result = common.NewJobResultSuccess()
		}
	case common.ACK_PROCESSOR:
		result = ack.ProcessAck()
	case common.EVENT_SEQUENCING:
		result = evntjobCmd.ExecuteS3DataSequencing(input)
	case common.ALERT_REPORT_PROCESSOR:
		result = auditReport.GenerateAuditReport(ctx)
	case common.DATA_HEALTH_SCORE_JOB:
		result = datahealthscore.CalculateDataHealthScore(ctx)
	case common.SILENT_DEVICE_ALERT:
		result = jobs.ProcessSilentDevices(ctx)
	case common.FHL_WINDOWS_ACTIVITY_CHECKER:
		result = jobs.CheckAndRestartFHLAgent(ctx)
	case common.DEVICE_INVENTORY_ALERT:
		result = cp_jobs.SendAlertForDeviceLevelAlert(ctx)
	case common.VOLUME_DEVIATION_ALERT:
		result = cp_jobs.SendAlertForVolumeDeviation(ctx)
	case common.VC_NO_REDUCTION_ALERT:
		result = cp_jobs.SendAlertForVCNoReduction(ctx)
	case common.DEPLOYMENT_DELAY_ALERT:
		result = cp_jobs.SendAlertForDeploymentDelayAlert(ctx)
	case common.VC_ALERTS:
		result = vc.SendAlertsForVCRules(ctx)
	case common.EVENT_FLOW_MONITORING:
		result = cp_jobs.SendAlertForPipelineFlowDeviation(ctx)
	case common.TRANSFORMATION_VALIDATOR:
		result = transformationCheckerUtility.ValidateTransformations(ctx)
	case common.UNPARSED_EVENTS_REPORT:
		result = unparsedReport.SendUnparsedEventsReport(ctx)
	default:
		logger.GetLogger().Panic("unknown job", zap.String("jobName", jobName))
	}

	// Check for errors and handle them once after the switch
	if len(result.Errors) > 0 {
		sendJobFailureAlert(ctx, jobName, input, result)
		logger.GetLogger().Error("failed to process job", zap.String("jobName", jobName))
		logger.GetLogger().Sync()
		os.Exit(1)
	} else {
		logger.GetLogger().Info("successfully processed job", zap.String("jobName", jobName))
		logger.GetLogger().Sync()
		os.Exit(0)
	}
}

// sendJobFailureAlert sends an engineering alert when a job fails
func sendJobFailureAlert(ctx context.Context, jobName string, input model.Message, result common.JobResult) {
	// Initialize alerts manager
	alertsManager, alertErr := alert.NewAlertsManager(ctx)
	if alertErr != nil {
		logger.GetLogger().Error("failed to create alerts manager for job failure alert", zap.Error(alertErr))
		return
	}
	defer alertsManager.Close(ctx)

	// Combine all error messages
	var errorMessages []string
	for _, jobError := range result.Errors {
		errorMessages = append(errorMessages, jobError.Message)
	}
	combinedMessage := strings.Join(errorMessages, "; ")

	alert, alertErr := alerts_async.NewAlert(
		alerts_async.Job,
		alerts_async.WithTitle(fmt.Sprintf("Databahn Job Failure Alert (%v)", jobName)),
		alerts_async.WithMessage(formatJobFailureMessage(jobName, combinedMessage, input)),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.JobFailure),
		alerts_async.WithEntityDetails("job", jobName, common.DatabahnDataPlaneId, common.DatabahnTenantId),
		alerts_async.WithErrorCode(alerts_async.DJFE10001, combinedMessage),
		alerts_async.WithAlertType(alerts_async.Internal),
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
		zap.String("error", combinedMessage))
}

// formatJobFailureMessage creates a detailed error message for the alert
func formatJobFailureMessage(jobName string, errorMessage string, input model.Message) string {
	message := "Job '" + jobName + "' failed with error: " + errorMessage

	// Add job parameters if available
	if input.RequestId != "" {
		message += ". Request ID: " + input.RequestId
	}
	if input.Source != "" {
		message += ". Source: " + input.Source
	}

	return message
}
