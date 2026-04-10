package logsource

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

// logSourceReportExtraHeaderColumns are appended after SQL result columns (order must match appendLogSourceStatColumns).
// Raw stats first; formatted columns last.
var logSourceReportExtraHeaderColumns = []string{
	"ingestion_stats", "destination_stats",
	"ingestion_stats_formatted", "destination_stats_formatted",
}

func WriteLogSourceReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing logsource report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))

	query, startTime, endTime, err := getQueryForLogSourceData(ctx, req)
	if err != nil {
		return err
	}
	err = getReportAndWriteToFile(ctx, req, query, startTime, endTime, file)
	if err != nil {
		return err
	}
	return nil
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

	pageSize := utils.GetEnvInt("LOGSOURCE_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true

	logsourceIdsToIngestionStats, err := getAggStatsForLogSourcePaginated(ctx, startTime, endTime, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	logsourceIdsToDestinationStats, err := getAggStatsForLogSourceToDestinationPaginated(ctx, startTime, endTime, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	destinationIdToNameMap, err := common.GetDestinationIdToNamesMap(ctx, req)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching destination names", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}

	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := common.GetRowsAndColumnsByQueryWithJoins(query, pageSize, offset, "ls.updated_at")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from logsource table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		if writeHeader {
			headerColumns := append(columns, logSourceReportExtraHeaderColumns...)
			writer = csv.NewWriter(file)
			err = writer.Write(headerColumns)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
				return err
			}
		}
		fetchedRowsCount, err := writeRowsToFileForLogSource(columns, rows, logsourceIdsToIngestionStats, logsourceIdsToDestinationStats, destinationIdToNameMap, writer)
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
func getQueryForLogSourceData(ctx context.Context, req models.AuditReport) (string, string, string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", "", err
	}

	filterToDbColumnMap := map[string]string{
		"scope":       "ls.scope",
		"status":      "ls.status",
		"vendor":      "ls.vendor",
		"device":      "ls.device",
		"logType":     "ls.log_type",
		"dataplaneId": "ls.data_plane_id",
		"tenant_id":   "ls.tenant_id",
	}

	whereClause, startTime, endTime := common.BuildQueryFromFilters(reportConfiguration, req.TenantId, filterToDbColumnMap)

	// Build the complete JOIN query with multi-fleet support
	query := fmt.Sprintf(`
		SELECT 
			ls.id,
			ls.name,
			ls.description,
			ls.device,
			ls.log_type,
			ls.reputation,
			ls.scope,
			ls.configuration,
			ls.status,
			ls.created_at,
			ls.updated_at,
			ls.vendor,
			ls.version,
			ls.replay_source,
			ls.timestamp_override_enabled,
			ls.timezone_normalization_enabled,
			STRING_AGG(DISTINCT f.name, ', ' ORDER BY f.name) as fleet_names,
			STRING_AGG(DISTINCT c.name, ', ' ORDER BY c.name) as connector_names,
			dp.name as dataplane_name,
			uc.email as created_by,
			uu.email as updated_by
		FROM log_source ls
		LEFT JOIN source_connector_mapping scm ON ls.id = scm.source_id
		LEFT JOIN connector c ON scm.connector_id = c.id
		LEFT JOIN fleet f ON c.fleet_id = f.id
		LEFT JOIN data_planes dp ON ls.data_plane_id = dp.id
		LEFT JOIN users uc ON ls.created_by = uc.id
		LEFT JOIN users uu ON ls.updated_by = uu.id
		WHERE %s
		GROUP BY ls.id, ls.name, ls.description, ls.device, ls.log_type, 
		         ls.reputation, ls.scope, ls.configuration::text, ls.status,
		         ls.created_at, ls.updated_at, ls.vendor, ls.version,
		         ls.replay_source, ls.timestamp_override_enabled,
		         ls.timezone_normalization_enabled, dp.name, 
		         uc.email, uu.email`, whereClause)

	logging.GetLoggerWithContext(ctx).Info("query for logsource data", zap.String("query", query), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	return query, startTime, endTime, nil
}
func writeRowsToFileForLogSource(columns []string, rows *sql.Rows, logsourceIdsToIngestionStats map[string]string, logsourceIdsToDestinationStats map[string]map[string]string, destinationIdToName map[string]string, writer *csv.Writer) (int, error) {

	fetchedRowsCount := 0
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		lsId := ""
		columnValue := ""
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}

		var row []string
		for i, col := range values {
			if columns[i] == "id" {
				lsId = string(*col.(*sql.RawBytes))
			}
			if columns[i] == "configuration" {
				row = append(row, "cannot write this column due to security reasons")
				continue
			}
			columnValue = string(*col.(*sql.RawBytes))
			row = append(row, columnValue)
		}
		row = appendLogSourceStatColumns(row, lsId, logsourceIdsToIngestionStats, logsourceIdsToDestinationStats, destinationIdToName)
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

// appendLogSourceStatColumns appends ingestion_stats, destination_stats, then ingestion_stats_formatted, destination_stats_formatted.
func appendLogSourceStatColumns(row []string, lsId string, ingestion map[string]string, destBySource map[string]map[string]string, destIdToName map[string]string) []string {
	raw := ingestion[lsId]
	destRaw := getDestinationStatsString(destBySource, lsId, destIdToName)
	row = append(row, raw, destRaw,
		formatEventCountReadable(raw),
		getDestinationStatsFormattedString(destBySource, lsId, destIdToName),
	)
	return row
}

func getDestinationStatsString(logsourceIdsToDestinationStats map[string]map[string]string, logSourceId string, destinationIdToName map[string]string) string {
	if destStats, ok := logsourceIdsToDestinationStats[logSourceId]; ok {
		var destinationStats []string
		for destId, value := range destStats {
			if name, exists := destinationIdToName[destId]; exists {
				destinationStats = append(destinationStats, fmt.Sprintf("%s: %s", name, value))
			} else {
				destinationStats = append(destinationStats, fmt.Sprintf("%s: %s", destId, value))
			}
		}
		return strings.Join(destinationStats, "\n")
	}
	return ""
}

func getDestinationStatsFormattedString(logsourceIdsToDestinationStats map[string]map[string]string, logSourceId string, destinationIdToName map[string]string) string {
	if destStats, ok := logsourceIdsToDestinationStats[logSourceId]; ok {
		var lines []string
		for destId, value := range destStats {
			formatted := formatEventCountReadable(value)
			if name, exists := destinationIdToName[destId]; exists {
				lines = append(lines, fmt.Sprintf("%s: %s", name, formatted))
			} else {
				lines = append(lines, fmt.Sprintf("%s: %s", destId, formatted))
			}
		}
		return strings.Join(lines, "\n")
	}
	return ""
}
