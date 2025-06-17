package destination

import (
	"context"
	"database/sql"
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

func FetchDestinationReport(ctx context.Context, req models.AuditReport, wg *sync.WaitGroup, parallelismCntrl chan struct{}, failedRequests *[]models.FailedRequests, successAlerts *[]alerts_common.AlertBaseObjectV2, failedRequestMutex *sync.Mutex, successAlertsMutex *sync.Mutex) {
	var file *os.File
	var writer *csv.Writer
	defer func() {
		file.Close()
		writer.Flush()
		wg.Done()
		<-parallelismCntrl
	}()

	var failedRequestsTemp []models.FailedRequests
	var successAlertsTemp []alerts_common.AlertBaseObjectV2
	logging.GetLogger().Info("Fetching destination report", zap.String("request_id", req.Id.String()), zap.String("tenant_id", req.TenantId))
	err := models.UpdateRequestStatus(config.GetDB(), req.Id.String(), consts.INPROGRESS)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while updating status to in progress", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		failedRequestsTemp = append(failedRequestsTemp, errRequest)
		return
	}

	file, writer, err = gatherDataAndWriteToFile(ctx, req, file, &failedRequestsTemp, writer)
	if err != nil {
		return
	}
	bucketName, objectKey := common.GetBucketNameAndObjectKey(req.Id.String())
	err = common.UploadFileToS3AndUpdateInDb(ctx, file, req, bucketName, objectKey)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while uploading file to s3", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return
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

func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, file *os.File, failedRequests *[]models.FailedRequests, writer *csv.Writer) (*os.File, *csv.Writer, error) {
	pageSize := utils.GetEnvInt("DESTINATION_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true

	query, file, err := getFileAndRequestConfig(ctx, req, file, failedRequests)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}
	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := getRowsAndColumnsFromAuditTable(pageSize, offset, query)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from audit table", zap.Error(err))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			*failedRequests = append(*failedRequests, errRequest)
			return nil, nil, err
		}
		fetchedRowsCount := 0

		if writeHeader {
			writer = csv.NewWriter(file)
			err = writer.Write(columns)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err))
				errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
				*failedRequests = append(*failedRequests, errRequest)
				return nil, nil, err
			}
		}

		fetchedRowsCount, err = common.WriteRowToTheFileOneByOne(columns, rows, writer, fetchedRowsCount)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err))
			errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
			*failedRequests = append(*failedRequests, errRequest)
			return nil, nil, err
		}
		if fetchedRowsCount < pageSize {
			break
		}
		offset += pageSize + 1
	}
	return file, writer, nil
}
func getFileAndRequestConfig(ctx context.Context, req models.AuditReport, file *os.File, failedRequests *[]models.FailedRequests) (string, *os.File, error) {
	file, err := common.CreateTempFile(req.Id.String())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}

	query, err := getQueryFromConfig(ctx, req, failedRequests)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}

	return query, file, err
}
func getRowsAndColumnsFromAuditTable(pageSize int, offset int, query string) (*sql.Rows, []string, error) {
	rows, err := config.GetDB().Table("destination").Limit(pageSize).Offset(offset).Where(query).Order("updated_at").Rows()
	if err != nil {
		return nil, nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	return rows, columns, nil
}
func getQueryFromConfig(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests) (string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		return "", err
	}
	var configData map[string]interface{}
	configData = reportConfiguration["filter"].(map[string]interface{})

	startTime := configData["startTime"].(string)
	endTime := configData["endTime"].(string)

	err = common.ValidateConfig(startTime, endTime)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error in validating startime and endtime", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}

	query := fmt.Sprintf("updated_at >= '%s' and updated_at <= '%s'", startTime, endTime)

	otherParamsAdded := false

	filterMappings := map[string]string{
		"scope":        "scope",
		"sources":      "log_source_id",
		"types":        "type",
		"destinations": "destination_id",
		"dataplaneId":  "data_plane_id",
	}

	for filterKey, dbField := range filterMappings {
		otherParamsAdded, query = common.UpdateQueryFromFilter(configData, otherParamsAdded, query, filterKey, dbField)
	}
	if otherParamsAdded {
		query += ")"
	}
	return query, nil
}
