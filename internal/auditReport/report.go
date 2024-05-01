package auditReport

import (
	"/github.com/databahn-ai/db-models/alerts_common"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"time"
)

func GenerateAuditReport(ctx context.Context) error {

	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("Handling audit report generation request")

	auditReportRequests, err := getAllReportRequests(config.GetDB())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting requests", zap.Error(err))
		return err
	}

	var failedRequests []FailedRequests
	var successAlerts []alerts_common.AlertEntityObject

	for _, req := range auditReportRequests {
		// update the status to in progress
		err = updateRequestStatus(config.GetDB(), req.Id.String(), STATUS_INPROGRESS)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while updating status to in progress", zap.Error(err))
			errRequest := NewFailedRequest(req.Id.String(), req.TenantId, req.Retries+1, err.Error())
			failedRequests = append(failedRequests, errRequest)
			continue
		}

		pageSize := utils.GetEnvInt("AUDIT_REPORT_PAGE_SIZE", 1000)
		offset := 0

		file, err := createTempFile(req.Id.String())
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err))
			errRequest := NewFailedRequest(req.Id.String(), req.TenantId, req.Retries+1, err.Error())
			failedRequests = append(failedRequests, errRequest)
			continue
		}
		defer file.Close()

		startTime, endTime, err := getConfigFromRequest(req)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while getting config from request", zap.Error(err))
			errRequest := NewFailedRequest(req.Id.String(), req.TenantId, req.Retries+1, err.Error())
			failedRequests = append(failedRequests, errRequest)
			continue
		}
		for {
			rows, columns, err := getRowsAndColumnsFromAuditTable(pageSize, offset, startTime, endTime)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while fetching data from audit table", zap.Error(err))
				errRequest := NewFailedRequest(req.Id.String(), req.TenantId, req.Retries+1, err.Error())
				failedRequests = append(failedRequests, errRequest)
				continue
			}
			fetchedRowsCount := 0
			writer := csv.NewWriter(file)
			defer writer.Flush()

			fetchedRowsCount, err = writeRowToTheFileOneByOne(columns, rows, writer, fetchedRowsCount)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err))
				errRequest := NewFailedRequest(req.Id.String(), req.TenantId, req.Retries+1, err.Error())
				failedRequests = append(failedRequests, errRequest)
				continue
			}
			if fetchedRowsCount < pageSize {
				break
			}
			offset += pageSize + 1
		}
		bucketName, objectKey := getBucketNameAndObjectKey(req.Id.String())

		// upload the file to s3
		err = uploadFileToS3(ctx, file.Name(), bucketName, objectKey)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while uploading file to s3", zap.Error(err))
			errRequest := NewFailedRequest(req.Id.String(), req.TenantId, req.Retries+1, err.Error())
			failedRequests = append(failedRequests, errRequest)
			continue
		}

		// get pre-signed link for the uploaded file
		downloadLink, err := getPresignedUrl(bucketName, objectKey)
		err = os.Remove(file.Name())
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while deleting temp file", zap.Error(err))
			errRequest := NewFailedRequest(req.Id.String(), req.TenantId, req.Retries+1, err.Error())
			failedRequests = append(failedRequests, errRequest)
			continue
		}

		// update the status to completed and link to database
		err = updateRequestStatusAndDownloadLink(config.GetDB(), req.Id.String(), STATUS_COMPLETED, downloadLink, time.Now().Add(time.Second*86400))
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while updating status to completed", zap.Error(err))
			errRequest := NewFailedRequest(req.Id.String(), req.TenantId, req.Retries+1, err.Error())
			failedRequests = append(failedRequests, errRequest)
			continue
		}
		// create alert entity object
		alertEntity := alerts_common.AlertEntityObject{
			EntityName:       req.Name,
			EntityId:         req.Id,
			EntityTenantUUId: utils.UUIDFromStringOrNil(req.TenantId),
		}
		successAlerts = append(successAlerts, alertEntity)
	}

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
func handleAlerts(ctx context.Context, successAlerts []alerts_common.AlertEntityObject, errorAlerts []alerts_common.AlertEntityObject) error {

	if len(successAlerts) > 0 {
		err := helper.SendAlertToControlPlane(ctx, successAlerts, SuccessTitle, SuccessTitle, AuditReportFunctionalityType, AuditReportFunctionality, alerts_common.InfoAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			return err
		}
	}
	if len(errorAlerts) > 0 {
		err := helper.SendAlertToControlPlane(ctx, errorAlerts, FailureTitle, FailureTitle, AuditReportFunctionalityType, AuditReportFunctionality, alerts_common.InfoAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			return err
		}
	}
	return nil
}
func handleErrorRequests(requests []FailedRequests) ([]alerts_common.AlertEntityObject, error) {

	var errorAlerts []alerts_common.AlertEntityObject
	for _, req := range requests {
		if req.Retry <= 3 {
			err := updateRequestStatusAndRetries(config.GetDB(), req.RequestId, STATUS_FAILED, req.Retry)
			if err != nil {
				return nil, err
			}
		} else {
			alertEntity := alerts_common.AlertEntityObject{
				EntityName:       req.RequestId,
				EntityId:         utils.UUIDFromStringOrNil(req.RequestId),
				EntityTenantUUId: utils.UUIDFromStringOrNil(req.TenantId),
			}
			errorAlerts = append(errorAlerts, alertEntity)
		}
	}
	return errorAlerts, nil
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
func getConfigFromRequest(req AuditReport) (string, string, error) {
	var configuration map[string]interface{}
	err := json.Unmarshal(req.Configuration, &configuration)
	if err != nil {
		return "", "", err
	}
	var configData map[string]interface{}
	configData = configuration["data"].(map[string]interface{})
	return configData["startTime"].(string), configData["endTime"].(string), nil
}
