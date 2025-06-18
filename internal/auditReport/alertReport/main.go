package alertReport

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
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"strconv"
	"strings"
	"sync"
)

func FetchAlertReport(ctx context.Context, req models.AuditReport, wg *sync.WaitGroup, parallelismCntrl chan struct{}, failedRequests *[]models.FailedRequests, successAlerts *[]alerts_common.AlertBaseObjectV2, failedRequestMutex *sync.Mutex, successAlertsMutex *sync.Mutex) {
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
	logging.GetLogger().Info("Fetching alert report", zap.String("request_id", req.Id.String()), zap.String("tenant_id", req.TenantId))
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
	query, file, writer, err := getFileAndRequestConfig(ctx, req, failedRequests, file, writer)
	if err != nil {
		return nil, nil, err
	}

	alertResponse, err := getAlertsFromOpenSearch(ctx, query)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting roi stats", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}
	err = writeAlertRowsToFile(alertResponse, writer)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}

	return file, writer, nil
}

func getFileAndRequestConfig(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests, file *os.File, writer *csv.Writer) (string, *os.File, *csv.Writer, error) {
	file, err := common.CreateTempFile(req.Name)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return "", nil, nil, err
	}

	// Write headers to the file
	headers := []string{"Id", "Title", "Criticality", "Message", "CreatedAt", "UpdatedAt", "FirstObservedAt", "LastObservedAt", "TenantId", "FunctionalityType", "Functionality", "FunctionalityEntityId", "FunctionalityEntityName", "Dismissed", "DismissedAt", "DismissedBy"}
	writer = csv.NewWriter(file)
	err = writer.Write(headers)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return "", nil, nil, err
	}

	query, err := getAlertReportConfigFromRequest(req)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return "", nil, nil, err
	}
	return query, file, writer, err
}
func writeAlertRowsToFile(alertResponse []statistics.AlertDocument, writer *csv.Writer) error {
	for _, alert := range alertResponse {
		row := []string{
			alert.Id,
			alert.Title,
			alert.Criticality,
			alert.Message,
			strconv.FormatInt(alert.CreatedAt, 10),
			strconv.FormatInt(alert.UpdatedAt, 10),
			strconv.FormatInt(alert.FirstObservedAt, 10),
			strconv.FormatInt(alert.LastObservedAt, 10),
			alert.TenantId,
			alert.FunctionalityType,
			alert.Functionality,
			alert.FunctionalityEntityId,
			alert.FunctionalityEntityName,
			strconv.FormatBool(alert.Dismissed),
			strconv.FormatInt(alert.DismissedAt, 10),
			alert.DismissedBy,
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

func getAlertReportConfigFromRequest(req models.AuditReport) (string, error) {
	var alertReportConfiguration map[string]interface{}
	var startTime, endTime string
	err := json.Unmarshal(req.AuditReportFilter, &alertReportConfiguration)
	if err != nil {
		return "", err
	}
	var configData map[string]interface{}
	configData = alertReportConfiguration["filter"].(map[string]interface{})
	if _, ok := configData["startTime"].(string); ok {
		startTime = configData["startTime"].(string)
	} else {
		return "", fmt.Errorf("startTime not found in config data or is a invalid string please check your config")
	}
	if _, ok := configData["endTime"].(string); ok {
		endTime = configData["endTime"].(string)
	} else {
		return "", fmt.Errorf("endTime not found in config data or is a invalid string please check your config")
	}
	dismissed, ok := configData["dismissed"].(bool)
	if !ok {
		dismissed = false
	}

	query := ""
	if dismissed {
		query = `updatedAt:>` + startTime + ` AND updatedAt:<` + endTime + ` AND tenantId: "` + req.TenantId + `"`
	} else {
		query = `updatedAt:>` + startTime + ` AND updatedAt:<` + endTime + ` AND tenantId: "` + req.TenantId + `"` + ` AND dismissed: false`

	}

	otherParamsAdded := false

	filterMappings := map[string]string{
		"status":        "status",
		"category":      "functionalityType",
		"severity":      "criticality",
		"functionality": "functionality",
	}

	// write a function to update other fields in the query if present in the config obj
	for k, v := range filterMappings {
		value, ok := configData[k]
		if !ok || len(value.([]interface{})) == 0 {
			logging.GetLogger().Info(fmt.Sprintf("no config not found for filter %s, ignoring this filter criteria", k))
			continue
		}
		if !otherParamsAdded {
			query += " AND "
			otherParamsAdded = true
		} else {
			query += " AND "
		}
		if sliceValue, ok := value.([]interface{}); ok {
			// Handle slice values
			stringSlice, err := convertInterfaceSliceToStringSlice(sliceValue)
			if err != nil {
				return "", err
			}
			if len(stringSlice) == 0 {
				continue
			}
			query += fmt.Sprintf("%s: (\"%s\")", v, strings.Join(stringSlice, "\" OR \""))
		} else {
			return "", fmt.Errorf("invalid type for %s in config data", k)
		}
	}

	return query, nil
}
func convertInterfaceSliceToStringSlice(interfaceSlice []interface{}) ([]string, error) {
	stringSlice := make([]string, len(interfaceSlice))
	for i, v := range interfaceSlice {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("element at index %d is not a string", i)
		}
		stringSlice[i] = str
	}
	return stringSlice, nil
}
