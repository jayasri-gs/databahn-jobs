package auditReport

import (
	"context"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/audit"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/consts"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/deviceInventory"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"sync"
)

func GenerateAuditReport(ctx context.Context) error {

	parallelism := utils.GetEnvInt("AUDIT_REPORT_PARALLELISM_CONTROL", 4)

	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("Handling audit report generation request")

	auditReportRequests, err := models.GetAllReportRequests(config.GetDB())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting requests", zap.Error(err))
		return err
	}

	var failedRequests []models.FailedRequests
	var successAlerts []helper.AlertBaseObjectV2

	failedRequestMutex := sync.Mutex{}
	successAlertsMutex := sync.Mutex{}

	wg := sync.WaitGroup{}
	parallelismCntrl := make(chan struct{}, parallelism)
	for _, req := range auditReportRequests {
		wg.Add(1)
		parallelismCntrl <- struct{}{}
		switch req.ReportType {
		case consts.AUDIT_REPORT:
			go audit.FetchAuditReport(ctx, req, &wg, parallelismCntrl, &failedRequests, &successAlerts, &failedRequestMutex, &successAlertsMutex)
		case consts.DEVICE_INVENTORY_REPORT:
			go deviceInventory.FetchDeviceInventory(ctx, req, &wg, parallelismCntrl, &failedRequests, &successAlerts, &failedRequestMutex, &successAlertsMutex)
		default:
			logging.GetLoggerWithContext(ctx).Error("invalid report type", zap.String("reportType", req.ReportType))
		}
	}
	wg.Wait()
	// handle failure requests
	errorAlerts, err := handleErrorRequests(failedRequests)
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

func handleAlerts(ctx context.Context, successAlerts []helper.AlertBaseObjectV2, errorAlerts []helper.AlertBaseObjectV2) error {

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
func handleErrorRequests(requests []models.FailedRequests) ([]helper.AlertBaseObjectV2, error) {

	var errorAlerts []helper.AlertBaseObjectV2
	for _, req := range requests {
		if req.Retry <= consts.MaxRetries {
			err := models.UpdateRequestStatusAndRetries(config.GetDB(), req.RequestId, consts.FAILED, req.Retry)
			if err != nil {
				return nil, err
			}
		}
		if req.Retry == consts.MaxRetries {
			alertEntity := helper.AlertBaseObjectV2{
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
