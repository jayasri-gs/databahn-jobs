package deviceInventory

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
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	opensearch "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func FetchDeviceInventory(ctx context.Context, req models.AuditReport, wg *sync.WaitGroup, parallelismCntrl chan struct{}, failedRequests *[]models.FailedRequests, successAlerts *[]alerts_common.AlertBaseObjectV2, failedRequestMutex *sync.Mutex, successAlertsMutex *sync.Mutex) {
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
	logging.GetLogger().Info("Fetching device inventory report", zap.String("request_id", req.Id.String()), zap.String("tenant_id", req.TenantId))
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
	sourceIdsToNames, query, file, writer, err := getFileAndRequestConfig(ctx, req, failedRequests, file, writer)
	if err != nil {
		return nil, nil, err
	}
	var searchAfter []any
	pageSize := utils.GetEnvInt("DEVICE_INVENTORY_REPORT_PAGE_SIZE", 1000)
	index := "db_insights_sights_sourcehostname_" + req.TenantId
	for {
		res, newSearchAfter, err := opensearch.SearchPaginated(ctx, opensearch.GetClient(), index, query, pageSize, searchAfter, []opensearch.Sort{{Field: "updated_at", Order: "asc"}})
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("index", index))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			*failedRequests = append(*failedRequests, errRequest)
			return nil, nil, err
		}

		var deviceInventoryList []statistics.DeviceInventoryDocument
		decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &deviceInventoryList})
		err = decoder.Decode(res)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while decoding response", zap.Error(err))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			*failedRequests = append(*failedRequests, errRequest)
			return nil, nil, err
		}

		err = writeDeviceInventoryRowsToFile(deviceInventoryList, sourceIdsToNames, writer)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			*failedRequests = append(*failedRequests, errRequest)
			return nil, nil, err
		}

		if len(res) == 0 || newSearchAfter == nil {
			break
		}
		searchAfter = newSearchAfter
	}
	return file, writer, nil
}

func getFileAndRequestConfig(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests, file *os.File, writer *csv.Writer) (map[string]string, string, *os.File, *csv.Writer, error) {
	logSources, err := helper.GetAllLogSourcesByTenantId(ctx, config.GetDB(), req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching log sources", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return map[string]string{}, "", nil, nil, err
	}
	var sourceIdsToNames = make(map[string]string)
	for _, source := range logSources {
		sourceIdsToNames[source.ID.String()] = source.Name
	}
	file, err = common.CreateTempFile(req.Name)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return map[string]string{}, "", nil, nil, err
	}
	headers := []string{"Hostname", "First Seen", "Last Seen", "Source Name", "Reputation"}
	writer = csv.NewWriter(file)
	err = writer.Write(headers)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return map[string]string{}, "", nil, nil, err
	}

	sources, startTime, endTime, err := getDeviceInventoryReportConfigFromRequest(req)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return map[string]string{}, "", nil, nil, err
	}
	query, err := getQueryFromFilters(sources, startTime, endTime, req.TenantId)
	return sourceIdsToNames, query, file, writer, err
}
func writeDeviceInventoryRowsToFile(deviceInventoryList []statistics.DeviceInventoryDocument, sourceIdsToNames map[string]string, writer *csv.Writer) error {
	for _, deviceInventory := range deviceInventoryList {
		firstSeen := time.Unix(0, deviceInventory.MinTime*int64(time.Millisecond)).Format(time.RFC3339)
		lastSeen := time.Unix(0, deviceInventory.MaxTime*int64(time.Millisecond)).Format(time.RFC3339)
		sourceName := sourceIdsToNames[deviceInventory.SourceId]
		row := []string{deviceInventory.Hostname, firstSeen, lastSeen, sourceName, deviceInventory.Reputation}
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

func getDeviceInventoryReportConfigFromRequest(req models.AuditReport) ([]string, string, string, error) {
	var deviceInventoryReportConfiguration map[string]interface{}
	var sources []string
	var startTime, endTime string
	err := json.Unmarshal(req.AuditReportFilter, &deviceInventoryReportConfiguration)
	if err != nil {
		return []string{}, "", "", err
	}
	var configData map[string]interface{}
	configData = deviceInventoryReportConfiguration["filter"].(map[string]interface{})
	if _, ok := configData["sources"]; ok {
		sources, _ = convertInterfaceSliceToStringSlice(configData["sources"].([]interface{}))
	}
	if _, ok := configData["startTime"].(string); ok {
		startTime = configData["startTime"].(string)
	}
	if _, ok := configData["endTime"].(string); ok {
		endTime = configData["endTime"].(string)
	}
	return sources, startTime, endTime, nil
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
func getQueryFromFilters(sources []string, startTime string, endTime string, tenantId string) (string, error) {
	q := "tenant_id: " + tenantId
	if len(sources) != 0 {
		q += ` AND source_id: ` + "(" + strings.Join(sources, " OR ") + ")"
	}
	if startTime != "" {
		// convert startTime to epoch
		t, err := time.Parse(time.RFC3339, startTime)
		if err != nil {
			fmt.Println("Error parsing time:", err)
			return "", err
		}
		startTimeEpoch := t.UnixMilli()
		q += ` AND max_time:>` + strconv.FormatInt(startTimeEpoch, 10)
	}
	if endTime != "" {
		// convert endTime to epoch
		t, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			fmt.Println("Error parsing time:", err)
			return "", err
		}
		endTimeEpoch := t.UnixMilli()
		q += ` AND max_time:<` + strconv.FormatInt(endTimeEpoch, 10)
	}
	return q, nil
}
