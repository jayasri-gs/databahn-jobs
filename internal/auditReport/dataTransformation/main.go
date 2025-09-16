package dataTransformation

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
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
		"status":       "dt.status",
		"sources":      "pls.log_source_id",
		"types":        "dt.type",
		"destinations": "pd.destination_id",
		"dataplaneId":  "dt.data_plane_id",
		"tenant_id":    "dt.tenant_id",
	}

	whereClause, startTime, endTime := common.BuildQueryFromFilters(reportConfiguration, req.TenantId, filterToDbColumnMap)

	// Build the complete JOIN query
	query := fmt.Sprintf(`
		SELECT 
			dt.id,
			dt.name,
			dt.description,
			dt.status,
			dt.type,
			dt.output_format,
			dt.include_raw_event,
			dt.created_at,
			dt.updated_at,
			ls.name as source_name,
			d.name as destination_name,
			dp.name as dataplane_name,
			uc.email as created_by,
			uu.email as updated_by,
			dt.transformation_destination_type
		FROM data_transformation dt
		LEFT JOIN pipelines p ON dt.pipeline_id = p.id
		LEFT JOIN pipeline_log_sources_mapping pls ON p.id = pls.pipeline_id
		LEFT JOIN log_source ls ON pls.log_source_id = ls.id
		LEFT JOIN pipeline_destinations_mapping pd ON p.id = pd.pipeline_id
		LEFT JOIN destination d ON pd.destination_id = d.id
		LEFT JOIN data_planes dp ON dt.data_plane_id = dp.id
		LEFT JOIN users uc ON dt.created_by::uuid = uc.id
		LEFT JOIN users uu ON dt.updated_by::uuid = uu.id
		WHERE %s`, whereClause)

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
		rows, columns, err := common.GetRowsAndColumnsByQueryWithJoins(query, pageSize, offset, "dt.updated_at")
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
