package auditReport

import (
	"context"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/agentReport"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/alertReport"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/audit"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
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
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"strconv"
	"sync"
	"time"
)

type ReportProcessor struct {
	successAlertChannel  chan alerts_common.AlertBaseObjectV2
	failedRequestChannel chan models.FailedRequests
}

func GenerateAuditReport(ctx context.Context) error {
	parallelism := utils.GetEnvInt("AUDIT_REPORT_PARALLELISM_CONTROL", 4)
	channelBufferSize := utils.GetEnvInt("AUDIT_REPORT_CHANNEL_BUFFER_SIZE", 4)

	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("handling audit report generation request")
	logging.GetLoggerWithContext(ctx).Info("initialising report processor object")

	reportProcessor := ReportProcessor{
		successAlertChannel:  make(chan alerts_common.AlertBaseObjectV2, channelBufferSize),
		failedRequestChannel: make(chan models.FailedRequests, channelBufferSize),
	}

	logging.GetLoggerWithContext(ctx).Info("fetching all audit report requests from db")
	auditReportRequests, err := models.GetAllReportRequests(config.GetDB())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting requests", zap.Error(err))
		return err
	}
	logging.GetLoggerWithContext(ctx).Info("fetched audit report requests from db", zap.Int("count", len(auditReportRequests)))
	if len(auditReportRequests) == 0 {
		logging.GetLoggerWithContext(ctx).Info("no audit report requests found")
		return nil
	}

	var successAlerts []alerts_common.AlertBaseObjectV2
	var errorRequests []models.FailedRequests
	go handleErrorRequestChannel(reportProcessor, &errorRequests)
	go handleSuccessRequestChannel(reportProcessor, &successAlerts)

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
		logging.GetLoggerWithContext(ctx).Error("error while handling error requests", zap.Error(err))
		return err
	}
	err = handleAlerts(ctx, successAlerts, errorAlerts)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while handling alerts", zap.Error(err))
		return err
	}
	return nil
}
func handleErrorRequestChannel(reportProcessor ReportProcessor, errorAlerts *[]models.FailedRequests) {
	for failedRequest := range reportProcessor.failedRequestChannel {
		*errorAlerts = append(*errorAlerts, failedRequest)
	}
}
func handleSuccessRequestChannel(reportProcessor ReportProcessor, successAlerts *[]alerts_common.AlertBaseObjectV2) {
	for successAlert := range reportProcessor.successAlertChannel {
		*successAlerts = append(*successAlerts, successAlert)
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

	file, err = common.CreateTempFile(req.Name)
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
	bucketName, objectKey := common.GetBucketNameAndObjectKey(req.Name + "_" + strconv.Itoa(int(time.Now().Unix())))
	err = common.UploadFileToS3AndUpdateInDb(ctx, file, req, bucketName, objectKey)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while uploading file to s3", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		reportProcessor.failedRequestChannel <- errRequest
		return
	} else {
		reportProcessor.successAlertChannel <- alerts_common.AlertBaseObjectV2{EntityName: req.Name, EntityId: utils.UUIDFromStringOrNil(req.Id.String()), EntityTenantUUId: utils.UUIDFromStringOrNil(req.TenantId), AlertType: alerts_common.AlertTypeExternalAndExternal}
	}
}

func handleAlerts(ctx context.Context, successAlerts []alerts_common.AlertBaseObjectV2, errorAlerts []alerts_common.AlertBaseObjectV2) error {

	if len(successAlerts) > 0 {
		err := helper.SendAlertToControlPlane(ctx, successAlerts, consts.SuccessTitle, consts.SuccessTitle, consts.AuditReportFunctionalityType, consts.AuditReportFunctionality, alerts_common.InfoAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			return err
		}
	}
	if len(errorAlerts) > 0 {
		err := helper.SendAlertToControlPlane(ctx, errorAlerts, consts.FailureTitle, consts.FailureTitle, consts.AuditReportFunctionalityType, consts.AuditReportFunctionality, alerts_common.InfoAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			return err
		}
	}
	return nil
}
func handleErrorRequests(requests []models.FailedRequests) ([]alerts_common.AlertBaseObjectV2, error) {

	var errorAlerts []alerts_common.AlertBaseObjectV2
	for _, req := range requests {
		if req.Retry <= consts.MaxRetries {
			err := models.UpdateRequestStatusAndRetries(config.GetDB(), req.RequestId, consts.FAILED, req.Retry)
			if err != nil {
				return nil, err
			}
		}
		if req.Retry == consts.MaxRetries {
			alertEntity := alerts_common.AlertBaseObjectV2{
				EntityName:       req.RequestId,
				EntityId:         utils.UUIDFromStringOrNil(req.RequestId),
				EntityTenantUUId: utils.UUIDFromStringOrNil(req.TenantId),
				AlertType:        alerts_common.AlertTypeExternalAndExternal,
			}
			errorAlerts = append(errorAlerts, alertEntity)
		}
	}
	return errorAlerts, nil
}
