package replayFilesReport

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"

	"github.com/databahn-ai/common-utils/utils"
	auditCommon "github.com/databahn-ai/databahn-jobs/internal/auditReport/common"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var csvHeaders = []string{"fileName", "size", "status", "startTime", "endTime", "error"}

type reportConfiguration struct {
	ReplayFilesReportConfig *replayFilesReportConfig `json:"replayFilesReportConfig"`
}

type replayFilesReportConfig struct {
	ReplayJobId string `json:"replayJobId"`
}

func WriteReplayFilesReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info(
		"writing replay files report to file",
		zap.String("request_id", req.Id.String()),
		zap.String("report_name", req.Name),
		zap.String("tenant_id", req.TenantId),
	)

	replayJobId, err := getReplayJobIdFromRequest(req)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error(
			"error while parsing replay files report configuration",
			zap.Error(err),
			zap.String("request_id", req.Id.String()),
			zap.String("report_name", req.Name),
			zap.String("tenant_id", req.TenantId),
		)
		return err
	}

	query := buildQuery(replayJobId, req.TenantId)
	if err := gatherDataAndWriteToFile(ctx, req, query, file); err != nil {
		return err
	}
	return nil
}

func getReplayJobIdFromRequest(req models.AuditReport) (string, error) {
	if len(req.ReportConfiguration) == 0 {
		return "", fmt.Errorf("missing report configuration")
	}

	var config reportConfiguration
	if err := json.Unmarshal(req.ReportConfiguration, &config); err != nil {
		return "", fmt.Errorf("invalid report configuration: %w", err)
	}
	if config.ReplayFilesReportConfig == nil {
		return "", fmt.Errorf("missing replay files report config")
	}
	if config.ReplayFilesReportConfig.ReplayJobId == "" {
		return "", fmt.Errorf("missing replay job id in replay files report config")
	}
	return config.ReplayFilesReportConfig.ReplayJobId, nil
}

func buildQuery(replayJobId, tenantId string) string {
	return fmt.Sprintf(`
		SELECT
			e.start_time,
			e.end_time,
			e.status,
			e.filename,
			e.error,
			e.size,
			e.lines
		FROM replay_job_executions e
		INNER JOIN replay_jobs j ON e.job_id = j.id
		WHERE e.job_id = '%s'::uuid
		  AND j.tenant_id = '%s'::uuid`,
		replayJobId, tenantId)
}

func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, query string, file *os.File) error {
	var writer *csv.Writer
	defer func() {
		if writer != nil {
			writer.Flush()
			if err := writer.Error(); err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while flushing writer", zap.Error(err))
			}
		}
	}()

	pageSize := utils.GetEnvInt("REPLAY_FILES_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true

	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := auditCommon.GetRowsAndColumnsByQueryWithJoins(
			query, pageSize, offset, "e.filename")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error(
				"error while fetching replay job executions",
				zap.Error(err),
				zap.String("request_id", req.Id.String()),
				zap.String("report_name", req.Name),
				zap.String("tenant_id", req.TenantId),
			)
			return err
		}

		if writeHeader {
			writer = csv.NewWriter(file)
			if err := writer.Write(csvHeaders); err != nil {
				rows.Close()
				logging.GetLoggerWithContext(ctx).Error(
					"error while writing headers to the file",
					zap.Error(err),
					zap.String("request_id", req.Id.String()),
					zap.String("report_name", req.Name),
					zap.String("tenant_id", req.TenantId),
				)
				return err
			}
		}

		fetchedRowsCount, err := writeExecutionRowsToFile(columns, rows, writer)
		rows.Close()
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error(
				"error while writing rows to the file",
				zap.Error(err),
				zap.String("request_id", req.Id.String()),
				zap.String("report_name", req.Name),
				zap.String("tenant_id", req.TenantId),
			)
			return err
		}
		if fetchedRowsCount < pageSize {
			break
		}
		offset += pageSize + 1
	}

	return nil
}

func writeExecutionRowsToFile(columns []string, rows *sql.Rows, writer *csv.Writer) (int, error) {
	columnIndex := map[string]int{}
	for i, column := range columns {
		columnIndex[column] = i
	}

	fetchedRowsCount := 0
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(sql.RawBytes)
	}

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}

		row := []string{
			stringValue(values, columnIndex, "filename"),
			sizeOrLinesValue(values, columnIndex),
			stringValue(values, columnIndex, "status"),
			stringValue(values, columnIndex, "start_time"),
			stringValue(values, columnIndex, "end_time"),
			stringValue(values, columnIndex, "error"),
		}
		if err := writer.Write(row); err != nil {
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

func stringValue(values []interface{}, columnIndex map[string]int, column string) string {
	idx, ok := columnIndex[column]
	if !ok || idx >= len(values) {
		return ""
	}
	return string(*values[idx].(*sql.RawBytes))
}

func sizeOrLinesValue(values []interface{}, columnIndex map[string]int) string {
	size := stringValue(values, columnIndex, "size")
	if size != "" {
		return size
	}
	return stringValue(values, columnIndex, "lines")
}
