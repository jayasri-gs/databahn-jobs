package enrichment

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

func WriteEnrichmentReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing enrichment report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))

	query, startTime, endTime, err := getQueryForEnrichmentReportData(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, query, startTime, endTime, file)
	if err != nil {
		return err
	}
	return nil
}

func getQueryForEnrichmentReportData(ctx context.Context, req models.AuditReport) (string, string, string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", "", err
	}

	filterToDbColumnMap := map[string]string{
		"status":       "e.status",
		"sources":      "e.source_id",
		"destinations": "e.destination_id",
		"dataplaneId":  "e.data_plane_id",
		"lookups":      "e.lookup_id",
		"tenant_id":    "e.tenant_id",
	}

	whereClause, startTime, endTime := common.BuildQueryFromFilters(reportConfiguration, req.TenantId, filterToDbColumnMap)

	// Build the complete JOIN query
	query := fmt.Sprintf(`
		SELECT 
			e.id,
			e.name,
			e.description,
			e.status,
			e.created_at,
			e.updated_at,
			ls.name as source_name,
			d.name as destination_name,
			l.name as lookup_name,
			e.config as config,
			uc.email as created_by,
			uu.email as updated_by
		FROM enrichment e
		LEFT JOIN log_source ls ON e.source_id = ls.id
		LEFT JOIN destination d ON e.destination_id = d.id
		LEFT JOIN lookup l ON e.lookup_id = l.id
		LEFT JOIN users uc ON e.created_by = uc.id
		LEFT JOIN users uu ON e.updated_by = uu.id
		WHERE %s`, whereClause)

	logging.GetLoggerWithContext(ctx).Info("query for enrichment report data", zap.String("query", query), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
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

	pageSize := utils.GetEnvInt("ENRICHMENT_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true
	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := common.GetRowsAndColumnsByQueryWithJoins(query, pageSize, offset, "e.updated_at")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from enrichment table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
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
