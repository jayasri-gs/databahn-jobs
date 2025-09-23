package auditReport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/agentReport"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/alertReport"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/audit"
	auditCommon "github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/consts"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/dataTransformation"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/destination"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/deviceInventory"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/enrichment"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/fleetReport"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/logsource"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/lookups"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/roiReport"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/volumeController"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/db-models/alerts_async"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type ReportProcessor struct {
	successAlertChannel  chan model.AuditReportAlert
	failedRequestChannel chan models.FailedRequests
}

func GenerateAuditReport(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	parallelism := utils.GetEnvInt("AUDIT_REPORT_PARALLELISM_CONTROL", 4)
	channelBufferSize := utils.GetEnvInt("AUDIT_REPORT_CHANNEL_BUFFER_SIZE", 4)

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error while creating alerts manager: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logging.GetLoggerWithContext(ctx).Error("error while creating alerts manager", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	defer func(logger *zap.Logger) {
		_ = logger.Sync()
		alertsManager.Close(ctx)
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("handling audit report generation request")
	logging.GetLoggerWithContext(ctx).Info("initialising report processor object")

	reportProcessor := ReportProcessor{
		successAlertChannel:  make(chan model.AuditReportAlert, channelBufferSize),
		failedRequestChannel: make(chan models.FailedRequests, channelBufferSize),
	}

	logging.GetLoggerWithContext(ctx).Info("fetching all audit report requests from db")
	auditReportRequests, err := models.GetAllReportRequests(config.GetDB())
	if err != nil {
		errorMsg := fmt.Sprintf("error while getting requests: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logging.GetLoggerWithContext(ctx).Error("error while getting requests", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}
	logging.GetLoggerWithContext(ctx).Info("fetched audit report requests from db", zap.Int("count", len(auditReportRequests)))
	if len(auditReportRequests) == 0 {
		logging.GetLoggerWithContext(ctx).Info("no audit report requests found")
		return common.NewJobResultSuccess()
	}

	var successAlerts []*alerts_async.Alert
	var errorRequests []models.FailedRequests
	go handleErrorRequestChannel(reportProcessor, &errorRequests)
	go handleSuccessRequestChannel(ctx, reportProcessor, &successAlerts)

	wg := sync.WaitGroup{}
	parallelismControl := make(chan struct{}, parallelism)
	for _, req := range auditReportRequests {
		wg.Add(1)
		parallelismControl <- struct{}{}
		go fetchReport(ctx, req, &wg, parallelismControl, reportProcessor)
	}
	wg.Wait()
	close(reportProcessor.successAlertChannel)
	close(reportProcessor.failedRequestChannel)

	errorAlerts, err := handleErrorRequests(errorRequests)
	if err != nil {
		errorMsg := fmt.Sprintf("error while handling error requests: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logging.GetLoggerWithContext(ctx).Error("error while handling error requests", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}
	err = handleAlerts(alertsManager, successAlerts, errorAlerts)
	if err != nil {
		errorMsg := fmt.Sprintf("error while handling alerts: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logging.GetLoggerWithContext(ctx).Error("error while handling alerts", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	if len(jobErrors) == 0 {
		logging.GetLoggerWithContext(ctx).Info("successfully completed audit report generation")
		return common.NewJobResultSuccess()
	} else {
		logging.GetLoggerWithContext(ctx).Info("audit report generation completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}
func handleErrorRequestChannel(reportProcessor ReportProcessor, errorAlerts *[]models.FailedRequests) {
	for failedRequest := range reportProcessor.failedRequestChannel {
		*errorAlerts = append(*errorAlerts, failedRequest)
	}
}
func handleSuccessRequestChannel(ctx context.Context, reportProcessor ReportProcessor, successAlerts *[]*alerts_async.Alert) {

	for successAlert := range reportProcessor.successAlertChannel {
		sucAlert, err := alerts_async.NewAlert(
			alerts_async.AuditReport,
			alerts_async.WithEntity(successAlert),
			alerts_async.WithCriticality(alerts_async.Info),
			alerts_async.WithFunctionalityType(alerts_async.AuditReportGeneration),
			alerts_async.WithTitle(consts.SuccessTitle),
			alerts_async.WithMessage(consts.SuccessTitle),
			alerts_async.WithErrorCode(alerts_async.DIIS10001, ""),
		)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while creating success alert", zap.Error(err))
		}
		*successAlerts = append(*successAlerts, sucAlert)
	}
}
func fetchReport(ctx context.Context, req models.AuditReport, wg *sync.WaitGroup, parallelismControl chan struct{}, reportProcessor ReportProcessor) {

	var file *os.File
	defer func() {
		if err := file.Close(); err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while closing file", zap.Error(err))
		}
		wg.Done()
		<-parallelismControl
	}()

	// update the status of report in db and print report type and other details
	logging.GetLoggerWithContext(ctx).Info("fetching report", zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))

	logging.GetLoggerWithContext(ctx).Info("updating request status to in progress", zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
	err := models.UpdateRequestStatus(config.GetDB(), req.Id.String(), consts.INPROGRESS)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while updating status to in progress", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		reportProcessor.failedRequestChannel <- errRequest
		return
	}

	file, err = auditCommon.CreateTempFile(req.Name)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		reportProcessor.failedRequestChannel <- errRequest
		return
	}

	switch req.ReportType {
	case consts.VOLUME_CONTROLLER_REPORT:
		err = volumeController.WriteVcReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching volume controller report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.TRANSFORMATION_REPORT:
		err = dataTransformation.WriteTransformationReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching transformation report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.LOOKUP_REPORT:
		err = lookups.WriteLookupReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching lookup report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.ENRICHMENT_REPORT:
		err = enrichment.WriteEnrichmentReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching enrichment report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.FLEET_REPORT:
		err = fleetReport.WriteFleetReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching fleet report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.AGENT_REPORT:
		err = agentReport.WriteAgentReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching agent report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.LOGSOURCE_REPORT:
		err = logsource.WriteLogSourceReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching log source report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.DESTINATION_REPORT:
		err = destination.WriteDestinationReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching destination report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.ROI_REPORT:
		err = roiReport.WriteROIReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching roi report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.ALERT_REPORT:
		err = alertReport.WriteAlertReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching alert report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.AUDIT_REPORT:
		err = audit.WriteAuditReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching audit report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	case consts.DEVICE_INVENTORY_REPORT:
		err = deviceInventory.WriteDeviceInventoryReportToFile(ctx, req, file)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching device inventory report", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId), zap.String("report_type", req.ReportType))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			reportProcessor.failedRequestChannel <- errRequest
			return
		}
	default:
		logging.GetLoggerWithContext(ctx).Error("unsupported report type", zap.String("report_type", req.ReportType), zap.String("request_id", req.Id.String()), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId))
		return
	}

	// upload file
	bucketName, objectKey := auditCommon.GetBucketNameAndObjectKey(req.Name + "_" + strconv.Itoa(int(time.Now().Unix())))
	err = auditCommon.UploadFileToS3AndUpdateInDb(ctx, file, req, bucketName, objectKey)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while uploading file to s3", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		reportProcessor.failedRequestChannel <- errRequest
		return
	} else {
		reportProcessor.successAlertChannel <- model.AuditReportAlert{
			EntityId:       req.Id.String(),
			EntityName:     req.Name,
			EntityTenantId: req.TenantId,
		}
	}
}

func handleAlerts(alertsManager *alert.AlertsManager, successAlerts []*alerts_async.Alert, errorAlerts []*alerts_async.Alert) error {
	if alertsManager == nil {
		return errors.New("alertsManager is nil")
	}

	if len(successAlerts) > 0 {
		alertsManager.SendAlerts(successAlerts)
	}
	if len(errorAlerts) > 0 {
		alertsManager.SendAlerts(errorAlerts)
	}
	return nil
}
func handleErrorRequests(requests []models.FailedRequests) ([]*alerts_async.Alert, error) {

	var errorAlerts []*alerts_async.Alert
	for _, req := range requests {
		if req.Retry <= consts.MaxRetries {
			err := models.UpdateRequestStatusAndRetries(config.GetDB(), req.RequestId, consts.FAILED, req.Retry)
			if err != nil {
				return nil, err
			}
		}
		if req.Retry == consts.MaxRetries {

			alert := model.AuditReportAlert{
				EntityId:       req.RequestId,
				EntityName:     req.Name,
				EntityTenantId: req.TenantId,
			}

			errorAlert, err := alerts_async.NewAlert(
				alerts_async.AuditReport,
				alerts_async.WithEntity(alert),
				alerts_async.WithCriticality(alerts_async.Info),
				alerts_async.WithFunctionalityType(alerts_async.AuditReportGeneration),
				alerts_async.WithTitle(consts.FailureTitle),
				alerts_async.WithMessage(consts.FailureTitle),
				alerts_async.WithErrorCode(alerts_async.DIIS20001, ""),
			)
			if err != nil {
				logging.GetLogger().Error("error while creating error alert", zap.Error(err), zap.String("request_id", req.RequestId), zap.String("request_name", req.Name), zap.String("tenant_id", req.TenantId))
			}

			errorAlerts = append(errorAlerts, errorAlert)
		}
	}
	return errorAlerts, nil
}
