package roiReport

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/consts"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"sync"
)

func FetchROIReport(ctx context.Context, req models.AuditReport, wg *sync.WaitGroup, parallelismCntrl chan struct{}, failedRequests *[]models.FailedRequests, successAlerts *[]alerts_common.AlertBaseObjectV2, failedRequestMutex *sync.Mutex, successAlertsMutex *sync.Mutex) {
	var file *os.File
	var writer *csv.Writer
	defer func() {
		writer.Flush()
		if err := writer.Error(); err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while flushing writer", zap.Error(err))
		}
		if err := file.Close(); err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while closing file", zap.Error(err))
		}
		wg.Done()
		<-parallelismCntrl
	}()

	var failedRequestsTemp []models.FailedRequests
	var successAlertsTemp []alerts_common.AlertBaseObjectV2
	logging.GetLogger().Info("Fetching roi report", zap.String("request_id", req.Id.String()), zap.String("tenant_id", req.TenantId))
	err := models.UpdateRequestStatus(config.GetDB(), req.Id.String(), consts.INPROGRESS)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while updating status to in progress", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		failedRequestsTemp = append(failedRequestsTemp, errRequest)
	}
	file, writer, err = gatherDataAndWriteToFile(ctx, req, failedRequests, file, writer)
	if err != nil {
		return
	}
	bucketName, objectKey := common.GetBucketNameAndObjectKey(req.Name)

	err = common.UploadFileToS3AndUpdateInDb(ctx, file, req, bucketName, objectKey)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while uploading file to s3", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		failedRequestsTemp = append(failedRequestsTemp, errRequest)
	} else {
		successAlertsTemp = append(successAlertsTemp, alerts_common.AlertBaseObjectV2{EntityName: req.Name, EntityId: utils.UUIDFromStringOrNil(req.Id.String()), EntityTenantUUId: utils.UUIDFromStringOrNil(req.TenantId), AlertType: alerts_common.AlertTypeExternalAndExternal})
	}

	// Lock the mutex before updating the success alerts
	successAlertsMutex.Lock()
	*successAlerts = append(*successAlerts, successAlertsTemp...)
	successAlertsMutex.Unlock()

	// Lock the mutex before updating the failed requests
	failedRequestMutex.Lock()
	*failedRequests = append(*failedRequests, failedRequestsTemp...)
	failedRequestMutex.Unlock()
}

func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests, file *os.File, writer *csv.Writer) (*os.File, *csv.Writer, error) {
	startTime, endTime, file, writer, err := getFileAndRequestConfig(ctx, req, failedRequests, file, writer)
	if err != nil {
		return nil, nil, err
	}

	// get destiantion id to names
	failedRequests, destinationIdToNameMap, err := common.GetDestinationIdToNamesMap(ctx, req, failedRequests)
	if err != nil {
		return nil, nil, err
	}

	failedRequests, logsourceIdToNameMap, err := common.GetLogSourceIdToNamesMap(ctx, req, failedRequests)
	if err != nil {
		return nil, nil, err
	}

	queryResponse, err := getAggStatsForLogSourceToDestinationPaginated(ctx, startTime, endTime, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting roi stats", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}
	err = writeRowsToFile(queryResponse, destinationIdToNameMap, logsourceIdToNameMap, writer)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}

	return file, writer, nil
}

func getFileAndRequestConfig(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests, file *os.File, writer *csv.Writer) (string, string, *os.File, *csv.Writer, error) {
	file, err := common.CreateTempFile(req.Name)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return "", "", nil, nil, err
	}
	headers := []string{"Source", "Destination", "Incoming Data ", "Outgoing Data", "Reduction percentage"}
	writer = csv.NewWriter(file)
	err = writer.Write(headers)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return "", "", nil, nil, err
	}

	startTime, endTime, err := getROIReportConfigFromRequest(req)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return "", "", nil, nil, err
	}
	return startTime, endTime, file, writer, err
}
func writeRowsToFile(queryResponse []Response, destinationIdsToName map[string]string, sourceIdsToNames map[string]string, writer *csv.Writer) error {
	for _, resp := range queryResponse {
		sourceName, ok := sourceIdsToNames[resp.LogSourceId]
		if !ok {
			sourceName = resp.LogSourceId
		}
		destinationName, ok := destinationIdsToName[resp.DestinationId]
		if !ok {
			destinationName = resp.DestinationId
		}
		row := []string{
			sourceName,
			destinationName,
			resp.Incoming,
			resp.Outgoing,
			resp.ReductionPercentage,
		}
		err := writer.Write(row)
		if err != nil {
			logging.GetLogger().Error("error while writing row to the file", zap.Error(err))
			return err
		}
	}
	logging.GetLogger().Info("Writing rows to the file completed")
	writer.Flush()
	return writer.Error()
}

func getROIReportConfigFromRequest(req models.AuditReport) (string, string, error) {
	var roiConfiguration map[string]interface{}
	var startTime, endTime string
	err := json.Unmarshal(req.AuditReportFilter, &roiConfiguration)
	if err != nil {
		return "", "", err
	}
	var configData map[string]interface{}
	configData = roiConfiguration["filter"].(map[string]interface{})
	if _, ok := configData["startTime"].(string); ok {
		startTime = configData["startTime"].(string)
	} else {
		return "", "", fmt.Errorf("startTime is not a valid string in the configuration")
	}
	if _, ok := configData["endTime"].(string); ok {
		endTime = configData["endTime"].(string)
	} else {
		return "", "", fmt.Errorf("endTime is not a valid string in the configuration")
	}
	return startTime, endTime, nil
}
