package audit

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
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
	"time"
)

func FetchAuditReport(ctx context.Context, req models.AuditReport, wg *sync.WaitGroup, parallelismCntrl chan struct{}, failedRequests *[]models.FailedRequests, successAlerts *[]alerts_common.AlertBaseObjectV2, failedRequestMutex *sync.Mutex, successAlertsMutex *sync.Mutex) {
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
	logging.GetLogger().Info("Fetching audit report", zap.String("request_id", req.Id.String()), zap.String("tenant_id", req.TenantId))
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
	pageSize := utils.GetEnvInt("AUDIT_REPORT_PAGE_SIZE", 1000)
	offset := 0
	startTime, endTime, file, err := getFileAndRequestConfig(ctx, req, file, failedRequests)
	if err != nil {
		return nil, nil, err
	}
	writeHeader := true

	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := getRowsAndColumnsFromAuditTable(pageSize, offset, startTime, endTime)
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

		fetchedRowsCount, err = writeRowToTheFileOneByOne(columns, rows, writer, fetchedRowsCount)
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
func getFileAndRequestConfig(ctx context.Context, req models.AuditReport, file *os.File, failedRequests *[]models.FailedRequests) (string, string, *os.File, error) {
	file, err := common.CreateTempFile(req.Id.String())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}

	startTime, endTime, err := getAuditReportConfigFromRequest(req)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}

	err = validateConfig(startTime, endTime)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error in validating startime and endtime", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
	}
	return startTime, endTime, file, err
}
func getRowsAndColumnsFromAuditTable(pageSize int, offset int, startTime string, endTime string) (*sql.Rows, []string, error) {
	rows, err := config.GetDB().Table("db_audit").Limit(pageSize).Offset(offset).Where("timestamp >= ? and timestamp <= ?", startTime, endTime).Order("timestamp").Rows()
	if err != nil {
		return nil, nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	return rows, columns, nil
}
func getAuditReportConfigFromRequest(req models.AuditReport) (string, string, error) {
	var auditReportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &auditReportConfiguration)
	if err != nil {
		return "", "", err
	}
	var configData map[string]interface{}
	configData = auditReportConfiguration["filter"].(map[string]interface{})
	return configData["startTime"].(string), configData["endTime"].(string), nil
}
func validateConfig(startTime string, endTime string) error {

	if startTime == "" || endTime == "" {
		return errors.New("start time and end time cannot be empty")
	}
	// Parse time strings into time.Time objects
	start, err := time.Parse(time.RFC3339, startTime)
	if err != nil {
		return err
	}

	end, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return err
	}

	// Compare time1 and time2
	if start.After(end) {
		return errors.New("startTime greater than endTime")
	}
	return nil
}
func writeRowToTheFileOneByOne(columns []string, rows *sql.Rows, writer *csv.Writer, fetchedRowsCount int) (int, error) {
	values := make([]interface{}, len(columns))

	for i := range values {
		values[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}

		var row []string
		for _, col := range values {
			// Convert column value to string
			columnValue := string(*col.(*sql.RawBytes))
			row = append(row, columnValue)
		}
		err := writer.Write(row)
		if err != nil {
			return 0, err
		}
		fetchedRowsCount++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return fetchedRowsCount, nil
}
