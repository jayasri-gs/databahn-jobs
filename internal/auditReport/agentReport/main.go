package agentReport

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

func WriteAgentReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing agent report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	query, startTime, endTime, err := getQueryForAgentData(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, query, startTime, endTime, file)
	if err != nil {
		return err
	}
	return nil
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

	pageSize := utils.GetEnvInt("AGENT_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true
	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := common.GetRowsAndColumnsByQueryWithJoins(query, pageSize, offset, "a.updated_at")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from agent table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
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
func getQueryForAgentData(ctx context.Context, req models.AuditReport) (string, string, string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", "", err
	}
	filterToDbColumnMap := map[string]string{
		"os":            "a.os",
		"status":        "a.status",
		"cpuArch":       "a.cpu_arch",
		"platform":      "a.platform",
		"kernelArch":    "a.kernel_arch",
		"kernelVersion": "a.kernel_version",
		"tenant_id":     "a.tenant_id",
	}

	whereClause, startTime, endTime := common.BuildQueryFromFilters(reportConfiguration, req.TenantId, filterToDbColumnMap)

	// Build the complete JOIN query
	query := fmt.Sprintf(`
		SELECT 
			a.id,
			a.name,
			a.description,
			a.hostname,
			a.os,
			a.platform,
			a.cpu_arch,
			a.cpu_count,
			a.kernel_arch,
			a.kernel_version,
			a.version,
			a.private_ip,
			a.public_ip,
			a.port,
			CASE 
				WHEN a.status = 'DELETED' THEN 'DELETED'
				WHEN now() - a.heartbeat_at > interval '30 minutes' THEN 'WARNING'
				ELSE a.status
			END as status,
			a.boot_time,
			a.uptime,
			a.is_upgrade_available,
			a.created_at,
			a.updated_at,
			a.heartbeat_at,
			a.runners,
			a.runners_log_level,
			a.diagnostic_location,
			dp.name as dataplane_name,
			uc.email as created_by,
			uu.email as updated_by
		FROM agent_node a
		    LEFT JOIN fleet f on a.fleet_id = f.id
		LEFT JOIN data_planes dp ON a.data_plane_id = dp.id
		LEFT JOIN users uc ON a.created_by = uc.id
		LEFT JOIN users uu ON a.updated_by = uu.id
		WHERE %s`, whereClause)

	logging.GetLoggerWithContext(ctx).Info("query for agent data", zap.String("query", query), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	return query, startTime, endTime, nil
}
func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, query string, startTime string, endTime string, file *os.File) error {
	err := getReportAndWriteToFile(ctx, req, query, file)
	if err != nil {
		return err
	}
	return nil
}
