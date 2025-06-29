package audit

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"time"
)

func WriteAuditReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing audit report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))

	startTime, endTime, err := getAuditReportConfigFromRequest(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, startTime, endTime, file)
	if err != nil {
		return err
	}
	return nil
}
func getAuditReportConfigFromRequest(ctx context.Context, req models.AuditReport) (string, string, error) {
	var auditReportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &auditReportConfiguration)
	if err != nil {
		return "", "", err
	}
	var configData map[string]interface{}
	configData = auditReportConfiguration["filter"].(map[string]interface{})
	startTime, ok := configData["startTime"].(string)
	if !ok {
		logging.GetLoggerWithContext(ctx).Error("startTime is not a string in audit report configuration", zap.Any("configData", configData))
		return "", "", errors.New("startTime is not a string in audit report configuration")
	}
	endTime, ok := configData["endTime"].(string)
	if !ok {
		logging.GetLoggerWithContext(ctx).Error("endTime is not a string in audit report configuration", zap.Any("configData", configData))
		return "", "", errors.New("endTime is not a string in audit report configuration")
	}
	if startTime == "" || endTime == "" {
		logging.GetLoggerWithContext(ctx).Error("startTime and endTime cannot be empty in audit report configuration", zap.Any("configData", configData))
		return "", "", errors.New("start time and end time cannot be empty")
	}
	// Parse time strings into time.Time objects
	start, err := time.Parse(time.RFC3339, startTime)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error parsing startTime", zap.Error(err), zap.String("startTime", startTime))
		return "", "", err
	}

	end, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error parsing endTime", zap.Error(err), zap.String("endTime", endTime))
		return "", "", err
	}
	// Compare time1 and time2
	if start.After(end) {
		logging.GetLoggerWithContext(ctx).Error("startTime is greater than endTime", zap.String("startTime", startTime), zap.String("endTime", endTime))
		return "", "", errors.New("startTime greater than endTime")
	}
	return configData["startTime"].(string), configData["endTime"].(string), nil
}
func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, startTime, endTime string, file *os.File) error {
	var writer *csv.Writer
	defer func() {
		if writer != nil {
			writer.Flush()
			if err := writer.Error(); err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while flushing writer", zap.Error(err))
			}
		}
	}()
	pageSize := utils.GetEnvInt("AUDIT_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true

	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := getRowsAndColumnsFromAuditTable(pageSize, offset, startTime, endTime, req.TenantId)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from audit table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}
		if writeHeader {
			writer = csv.NewWriter(file)
			err = writer.Write(columns)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
				return err
			}
		}

		fetchedRowsCount, err := common.WriteRowsToFileForDbReportTypeWithoutTimeFilters(columns, rows, writer)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}
		if fetchedRowsCount < pageSize {
			break
		}
		offset += pageSize + 1
	}
	return nil
}
func getRowsAndColumnsFromAuditTable(pageSize int, offset int, startTime string, endTime string, tenantId string) (*sql.Rows, []string, error) {
	rows, err := config.GetDB().Table("db_audit").Limit(pageSize).Offset(offset).Where("tenant_uuid = ? and timestamp >= ? and timestamp <= ?", tenantId, startTime, endTime).Order("timestamp").Rows()
	if err != nil {
		return nil, nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	return rows, columns, nil
}
