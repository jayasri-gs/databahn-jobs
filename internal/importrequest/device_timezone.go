package importrequest

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/importrequest/consts"
	"github.com/databahn-ai/databahn-jobs/internal/importrequest/models"
	"github.com/databahn-ai/databahn-jobs/internal/insights"
	"github.com/databahn-ai/databahn-jobs/internal/store/objstore"
	osstore "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
)

const (
	deviceTimezoneBatchSize = 100
	requiredColumnCount     = 2
)

type csvRow struct {
	RowNumber int
	Hostname  string
	Timezone  string
}

type deviceDocUpdate struct {
	DeviceTimezone       string `json:"device_timezone"`
	TimezoneUpdatedAt    int64  `json:"timezone_updated_at"`
	TimezoneUpdatedBy    string `json:"timezone_updated_by"`
	TimezoneUpdateReason string `json:"timezone_update_reason"`
	UpdatedAt            int64  `json:"updated_at"`
}

type bulkItemOutcome struct {
	deviceID string
	status   int
	reason   string
}

func processDeviceTimezoneMapping(ctx context.Context, req models.ImportRequest, log *zap.Logger) (*models.ImportStats, error) {
	if req.FileType != consts.FileTypeCSV {
		return nil, fmt.Errorf("unsupported file type %q", req.FileType)
	}
	if strings.TrimSpace(req.FilePath) == "" {
		return nil, fmt.Errorf("missing file path")
	}

	bucket := objstore.GetBucket(objstore.BucketArtifacts)
	if bucket == "" {
		return nil, fmt.Errorf("artifacts bucket not configured")
	}

	data, err := readImportFileWithRetry(ctx, bucket, req.FilePath)
	if err != nil {
		return nil, err
	}

	rows, skippedBlank, err := parseDeviceTimezoneCSV(data, req.HasHeaderRow())
	if err != nil {
		return nil, err
	}

	stats := &models.ImportStats{
		TotalRows:   len(rows),
		SkippedRows: skippedBlank,
	}

	tenantID := req.TenantID.String()
	updatedBy := req.CreatedBy.String()
	now := time.Now().UTC().UnixMilli()

	var updateRows []csvRow
	for _, row := range rows {
		if err := validateHostname(row.Hostname); err != nil {
			recordRowFailure(log, stats, row, err.Error())
			continue
		}
		if strings.TrimSpace(row.Timezone) != "" {
			if err := insights.ValidateIANATimezone(row.Timezone); err != nil {
				recordRowFailure(log, stats, row, err.Error())
				continue
			}
		}
		updateRows = append(updateRows, row)
	}

	if len(updateRows) == 0 {
		return stats, nil
	}

	cli := osstore.GetClient()
	indexName := insights.DeviceIndexName(tenantID)

	for start := 0; start < len(updateRows); start += deviceTimezoneBatchSize {
		end := start + deviceTimezoneBatchSize
		if end > len(updateRows) {
			end = len(updateRows)
		}
		batch := updateRows[start:end]
		outcomes, err := bulkSetDeviceTimezones(ctx, cli, indexName, tenantID, batch, updatedBy, now)
		if err != nil {
			return stats, fmt.Errorf("bulk update failed for batch starting at row %d: %w", batch[0].RowNumber, err)
		}
		applyBulkOutcomes(log, stats, tenantID, batch, outcomes)
	}

	log.Info("device timezone mapping processed",
		zap.Int("totalRows", stats.TotalRows),
		zap.Int("processedRows", stats.ProcessedRows),
		zap.Int("skippedRows", stats.SkippedRows),
		zap.Int("failedRows", stats.FailedRows))

	return stats, nil
}

func parseDeviceTimezoneCSV(data []byte, hasHeaders bool) ([]csvRow, int, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1

	var rows []csvRow
	skippedBlank := 0
	rowNumber := 0
	headerValidated := false

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("invalid CSV file format: %w", err)
		}
		rowNumber++
		if isBlankCSVRow(record) {
			skippedBlank++
			continue
		}
		if len(record) != requiredColumnCount {
			return nil, 0, fmt.Errorf("CSV row %d must contain exactly %d columns", rowNumber, requiredColumnCount)
		}

		if hasHeaders && !headerValidated {
			headerValidated = true
			continue
		}

		hostname := strings.TrimSpace(record[0])
		timezone := strings.TrimSpace(record[1])
		if hostname == "" {
			return nil, 0, fmt.Errorf("CSV row %d is missing hostname in the first column", rowNumber)
		}
		rows = append(rows, csvRow{
			RowNumber: rowNumber,
			Hostname:  hostname,
			Timezone:  timezone,
		})
	}

	if rowNumber == 0 {
		return nil, 0, fmt.Errorf("CSV file is empty")
	}
	if hasHeaders && !headerValidated {
		return nil, 0, fmt.Errorf("CSV file is missing a header row")
	}
	if len(rows) == 0 {
		return nil, 0, fmt.Errorf("CSV file must contain at least one data row")
	}
	return rows, skippedBlank, nil
}

func validateHostname(hostname string) error {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return fmt.Errorf("hostname is required")
	}
	return nil
}

func isBlankCSVRow(record []string) bool {
	if len(record) == 0 {
		return true
	}
	for _, value := range record {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

func recordRowFailure(log *zap.Logger, stats *models.ImportStats, row csvRow, reason string) {
	stats.FailedRows++
	log.Warn("import row failed",
		zap.Int("rowNumber", row.RowNumber),
		zap.String("hostname", row.Hostname),
		zap.String("timezone", row.Timezone),
		zap.String("reason", reason),
	)
}

func applyBulkOutcomes(log *zap.Logger, stats *models.ImportStats, tenantID string, batch []csvRow, outcomes []bulkItemOutcome) {
	outcomeByID := make(map[string]bulkItemOutcome, len(outcomes))
	for _, outcome := range outcomes {
		outcomeByID[outcome.deviceID] = outcome
	}
	for _, row := range batch {
		deviceID := insights.DeviceId(tenantID, row.Hostname)
		outcome, ok := outcomeByID[deviceID]
		if !ok {
			recordRowFailure(log, stats, row, "bulk update outcome missing")
			continue
		}
		if outcome.status >= 200 && outcome.status < 300 {
			stats.ProcessedRows++
			continue
		}
		reason := outcome.reason
		if reason == "" {
			reason = fmt.Sprintf("bulk update failed with status %d", outcome.status)
		}
		recordRowFailure(log, stats, row, reason)
	}
}

func bulkSetDeviceTimezones(
	ctx context.Context,
	cli *opensearch.Client,
	indexName string,
	tenantID string,
	rows []csvRow,
	updatedBy string,
	updatedAt int64,
) ([]bulkItemOutcome, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	buff := new(bytes.Buffer)
	for _, row := range rows {
		deviceID := insights.DeviceId(tenantID, row.Hostname)
		if _, err := fmt.Fprintf(buff, "{\"update\": {\"_id\": %q}}\n", deviceID); err != nil {
			return nil, err
		}
		doc := deviceDocUpdate{
			DeviceTimezone:       row.Timezone,
			TimezoneUpdatedAt:    updatedAt,
			TimezoneUpdatedBy:    updatedBy,
			TimezoneUpdateReason: insights.TimezoneUpdateReasonManual,
			UpdatedAt:            updatedAt,
		}
		body, err := json.Marshal(map[string]deviceDocUpdate{"doc": doc})
		if err != nil {
			return nil, err
		}
		buff.Write(body)
		buff.WriteByte('\n')
	}

	request := opensearchapi.BulkRequest{
		Index: indexName,
		Body:  buff,
	}
	resp, err := request.Do(ctx, cli)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return nil, fmt.Errorf("bulk request failed: %s", resp.String())
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	bulkResp := osstore.BulkResponse{}
	if err := json.Unmarshal(bodyBytes, &bulkResp); err != nil {
		return nil, err
	}

	outcomes := make([]bulkItemOutcome, 0, len(bulkResp.Items))
	for _, item := range bulkResp.Items {
		for _, result := range item {
			reason := result.Error.Reason
			if reason == "" && result.Error.Type != "" {
				reason = result.Error.Type
			}
			outcomes = append(outcomes, bulkItemOutcome{
				deviceID: result.Id,
				status:   result.Status,
				reason:   reason,
			})
		}
	}
	return outcomes, nil
}

func readImportFileWithRetry(ctx context.Context, bucket, filePath string) ([]byte, error) {
	client := objstore.GetClient()
	var lastErr error

	for attempt := 1; attempt <= consts.MaxFileDownloadRetries; attempt++ {
		data, err := client.Get(ctx, bucket, filePath)
		if err == nil {
			return data, nil
		}
		lastErr = err
		if attempt < consts.MaxFileDownloadRetries {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
	}

	return nil, fmt.Errorf(
		"failed to read import file from object store after %d attempts: %w",
		consts.MaxFileDownloadRetries,
		lastErr,
	)
}

func importRequestLogFields(req models.ImportRequest) []zap.Field {
	fields := []zap.Field{
		zap.String("importRequestId", req.ID.String()),
		zap.String("tenantId", req.TenantID.String()),
		zap.String("name", req.Name),
		zap.String("importType", req.ImportType),
		zap.String("status", req.Status),
		zap.String("fileName", req.FileName),
		zap.String("filePath", req.FilePath),
		zap.String("fileStorage", req.FileStorage),
		zap.String("fileType", req.FileType),
		zap.Bool("hasHeaders", req.HasHeaderRow()),
		zap.Int("retries", req.Retries),
		zap.String("createdBy", req.CreatedBy.String()),
	}
	if req.StartedAt != nil {
		fields = append(fields, zap.Time("startedAt", *req.StartedAt))
	}
	if req.CompletedAt != nil {
		fields = append(fields, zap.Time("completedAt", *req.CompletedAt))
	}
	if len(req.Stats) > 0 {
		fields = append(fields, zap.ByteString("stats", req.Stats))
	}
	if req.ErrorMessage != "" {
		fields = append(fields, zap.String("errorMessage", req.ErrorMessage))
	}
	return fields
}

func logImportRequestAfterProcessing(log *zap.Logger, req models.ImportRequest, stats *models.ImportStats, status string, errMsg string) {
	fields := importRequestLogFields(req)
	fields = append(fields, zap.String("finalStatus", status))
	if stats != nil {
		fields = append(fields,
			zap.Int("totalRows", stats.TotalRows),
			zap.Int("processedRows", stats.ProcessedRows),
			zap.Int("skippedRows", stats.SkippedRows),
			zap.Int("failedRows", stats.FailedRows),
		)
	}
	if errMsg != "" {
		fields = append(fields, zap.String("errorMessage", errMsg))
	}
	log.Info("import request processing finished", fields...)
}
