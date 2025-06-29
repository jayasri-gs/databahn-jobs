package logsource

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"strings"
)

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
		rows, columns, err := common.GetRowsAndColumnsByQueryFromTable("log_source", query, pageSize, offset)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from logsource table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		if writeHeader {
			headerColumns := append(columns, "ingestion_stats", "destination_stats")
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
	filterToDbColumnMap := map[string]string{
		"scope":       "scope",
		"status":      "status",
		"vendor":      "vendor",
		"device":      "device",
		"logType":     "log_type",
		"dataplaneId": "data_plane_id",
	}

	startTime, endTime, query, err := common.GetDbQueryWithTimeFilters(req, filterToDbColumnMap)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting query from config", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", "", "", err
	}
	if startTime == "" || endTime == "" {
		return "", "", "", fmt.Errorf("startTime or endTime missing in logsource report config")
	}
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
		row = append(row, logsourceIdsToIngestionStats[lsId])
		row = append(row, getDestinationStatsString(logsourceIdsToDestinationStats, lsId, destinationIdToName))
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
		return fmt.Sprintf("%v", strings.Join(destinationStats, "\n"))
	}
	return ""
}
