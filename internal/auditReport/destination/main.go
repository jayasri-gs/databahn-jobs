package destination

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func WriteDestinationReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing destination report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))

	query, startTime, endTime, err := getQueryForDestinationReportData(ctx, req)
	if err != nil {
		return err
	}
	err = getReportAndWriteToFile(ctx, req, query, startTime, endTime, file)
	if err != nil {
		return err
	}
	return nil
}
func getQueryForDestinationReportData(ctx context.Context, req models.AuditReport) (string, string, string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", "", err
	}

	filterToDbColumnMap := map[string]string{
		"scope":           "d.scope",
		"status":          "d.status",
		"destinationType": "d.destination_type",
		"forwardDataType": "d.forward_data_type",
		"dataplaneId":     "d.data_plane_id",
		"tenant_id":       "d.tenant_id",
	}

	whereClause, startTime, endTime := common.BuildQueryFromFilters(reportConfiguration, req.TenantId, filterToDbColumnMap)

	// Build the complete JOIN query
	query := fmt.Sprintf(`
		SELECT 
			d.id,
			d.name,
			d.description,
			d.destination_type,
			d.forward_data_type,
			d.configuration,
			d.status,
			d.scope,
			d.created_at,
			d.updated_at,
			dp.name as dataplane_name,
			uc.email as created_by,
			uu.email as updated_by
		FROM destination d
		LEFT JOIN data_planes dp ON d.data_plane_id = dp.id
		LEFT JOIN users uc ON d.created_by::uuid = uc.id
		LEFT JOIN users uu ON d.updated_by::uuid = uu.id
		WHERE %s`, whereClause)

	logging.GetLoggerWithContext(ctx).Info("query for destination report data", zap.String("query", query), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	return query, startTime, endTime, nil
}

func getReportAndWriteToFile(ctx context.Context, req models.AuditReport, query, startTime, endTime string, file *os.File) error {
	var writer *csv.Writer
	defer func() {
		if writer != nil {
			writer.Flush()
			if err := writer.Error(); err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while flushing writer", zap.Error(err))
			}
		}
	}()

	pageSize := utils.GetEnvInt("DESTINATION_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true

	dstIdsToLogSourceStatsMap, err := getAggStatsForDestinationToLogSourcePaginated(ctx, startTime, endTime, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	logsourceIdToNameMap, err := common.GetLogSourceIdToNamesMap(ctx, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching logsource names", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := common.GetRowsAndColumnsByQueryWithJoins(query, pageSize, offset, "d.updated_at")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from destination table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		if writeHeader {
			headerColumns := append(columns, "destination_logsource_stats")
			writer = csv.NewWriter(file)
			err = writer.Write(headerColumns)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
				return err
			}
		}
		fetchedRowsCount, err := writeRowsToFileForDestination(columns, rows, dstIdsToLogSourceStatsMap, logsourceIdToNameMap, writer)
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
func writeRowsToFileForDestination(columns []string, rows *sql.Rows, dstIdsToLogSourceStatsMap map[string]map[string]string, logsourceIdToNameMap map[string]string, writer *csv.Writer) (int, error) {

	fetchedRowsCount := 0
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		dstId := ""
		columnValue := ""
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}

		var row []string
		for i, col := range values {
			if columns[i] == "id" {
				dstId = string(*col.(*sql.RawBytes))
			}
			if columns[i] == "configuration" {
				row = append(row, "cannot write this column due to security reasons")
				continue
			}
			columnValue = string(*col.(*sql.RawBytes))
			row = append(row, columnValue)
		}
		row = append(row, getLogSourceStatsString(dstIdsToLogSourceStatsMap, dstId, logsourceIdToNameMap))
		err := writer.Write(row)
		if err != nil {
			logging.GetLoggerWithContext(context.Background()).Error("error while writing row to the file", zap.Error(err))
			return 0, err
		}
		fetchedRowsCount++
	}
	writer.Flush()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return fetchedRowsCount, nil
}
func getLogSourceStatsString(dstIdsToLogSourceStatsMap map[string]map[string]string, destinationId string, logsourceIdToNameMap map[string]string) string {
	if lsStats, ok := dstIdsToLogSourceStatsMap[destinationId]; ok {
		var logSourceStats []string
		for lsId, value := range lsStats {
			if name, exists := logsourceIdToNameMap[lsId]; exists {
				logSourceStats = append(logSourceStats, fmt.Sprintf("%s: %s", name, value))
			} else {
				logSourceStats = append(logSourceStats, fmt.Sprintf("%s: %s", lsId, value))
			}
		}
		return fmt.Sprintf("%v", strings.Join(logSourceStats, "\n"))
	}
	return ""
}
