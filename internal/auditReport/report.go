package auditReport

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
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
	for _, req := range auditReportRequests {
		// update the status to in progress
		err = updateRequestStatus(config.GetDB(), req.Id.String(), STATUS_INPROGRESS)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while updating status to in progress", zap.Error(err))
			return err
		}

		pageSize := utils.GetEnvInt("AUDIT_REPORT_PAGE_SIZE", 1000)
		offset := 0

		file, err := createTempFile(req.Id.String())
		defer file.Close()
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while creating temp file", zap.Error(err))
			return err
		}

		startTime, endTime, err := getConfigFromRequest(req)
		for {
			rows, columns, err := getRowsAndColumnsFromAuditTable(pageSize, offset, startTime, endTime)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while fetching data from audit table", zap.Error(err))
				return err
			}
			fetchedRowsCount := 0
			writer := csv.NewWriter(file)
			defer writer.Flush()

			fetchedRowsCount, err = writeRowToTheFileOneByOne(columns, rows, writer, fetchedRowsCount)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err))
				return err
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
			return err
		}

		// get pre-signed link for the uploaded file
		downloadLink, err := getPresignedUrl(bucketName, objectKey)
		err = os.Remove(file.Name())
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while deleting temp file", zap.Error(err))
			return err
		}

		// update the status to completed and link to database
		err = updateRequestStatusAndDownloadLink(config.GetDB(), req.Id.String(), STATUS_COMPLETED, downloadLink)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while updating status to completed", zap.Error(err))
			return err
		}
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
