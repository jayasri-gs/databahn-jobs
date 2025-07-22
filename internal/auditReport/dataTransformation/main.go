package dataTransformation

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
)

func WriteTransformationReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing transformation report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))

	query, startTime, endTime, err := getQueryForTransformationReportData(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, query, startTime, endTime, file)
	if err != nil {
		return err
	}
	return nil
}

func getQueryForTransformationReportData(ctx context.Context, req models.AuditReport) (string, string, string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", "", err
	}

	filterToDbColumnMap := map[string]string{
		"status":       "status",
		"sources":      "log_source_id",
		"types":        "type",
		"destinations": "destination_id",
		"dataplaneId":  "data_plane_id",
	}

	query, startTime, endTime := common.BuildQueryFromFilters(reportConfiguration, req.TenantId, filterToDbColumnMap)
	logging.GetLoggerWithContext(ctx).Info("query for transformation report data", zap.String("query", query), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	return query, startTime, endTime, nil
}

func getReportAndWriteToFile(ctx context.Context, req models.AuditReport, query string, file *os.File) error {

	var writer *csv.Writer
	defer func() {
		if writer != nil {
			writer.Flush()
			if err := writer.Error(); err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while flushing writer", zap.Error(err))
			}
		}
	}()

	pageSize := utils.GetEnvInt("DATA_TRANSFORMATION_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true
	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := common.GetRowsAndColumnsByQueryFromTable("data_transformation", query, pageSize, offset)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from data transformation table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
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

func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, query string, startTime string, endTime string, file *os.File) error {
	err := getReportAndWriteToFile(ctx, req, query, file)
	if err != nil {
		return err
	}
	return nil
}
