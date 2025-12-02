package contentStudioRules

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
	"github.com/databahn-ai/databahn-jobs/internal/config"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func WriteContentStudioRulesReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing content studio rules report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))

	query, err := getQueryForContentStudioRulesData(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, query, file)
	if err != nil {
		return err
	}
	return nil
}

func getQueryForContentStudioRulesData(ctx context.Context, req models.AuditReport) (string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", err
	}

	filterToDbColumnMap := map[string]string{
		"source_vendor":   "rcs.source_vendor",
		"source_device":   "rcs.source_device",
		"source_log_type": "rcs.source_log_type",
		"type":            "rcs.type",
		"action_type":     "rcs.action_type",
		"scope":           "rcs.scope",
	}

	// Build query parts manually since rule_content_studio doesn't have tenant_id
	var queryParts []string

	// Handle and_filters
	if andFilters, ok := reportConfiguration["and_filters"].(map[string]interface{}); ok {
		for key, val := range andFilters {
			dbCol, ok := filterToDbColumnMap[key]
			if !ok {
				continue
			}
			arr, ok := val.([]interface{})
			if !ok || len(arr) == 0 {
				continue
			}
			var vals []string
			for _, v := range arr {
				if s, ok := v.(string); ok {
					vals = append(vals, fmt.Sprintf("'%s'", s))
				}
			}
			if len(vals) > 0 {
				queryParts = append(queryParts, fmt.Sprintf("%s IN (%s)", dbCol, strings.Join(vals, ", ")))
			}
		}
	}

	// Handle or_filters (kept for code consistency with other reports, but typically not used)
	var orGroup []string
	if orFilters, ok := reportConfiguration["or_filters"].(map[string]interface{}); ok {
		for key, val := range orFilters {
			dbCol, ok := filterToDbColumnMap[key]
			if !ok {
				continue
			}
			arr, ok := val.([]interface{})
			if !ok || len(arr) == 0 {
				continue
			}
			var vals []string
			for _, v := range arr {
				if s, ok := v.(string); ok {
					vals = append(vals, fmt.Sprintf("'%s'", s))
				}
			}
			if len(vals) > 0 {
				orGroup = append(orGroup, fmt.Sprintf("%s IN (%s)", dbCol, strings.Join(vals, ", ")))
			}
		}
	}
	if len(orGroup) > 0 {
		queryParts = append(queryParts, fmt.Sprintf("(%s)", strings.Join(orGroup, " OR ")))
	}

	// Build WHERE clause
	whereClause := "1=1" // Default to show all if no filters
	if len(queryParts) > 0 {
		whereClause = strings.Join(queryParts, " AND ")
	}

	// Build the complete query - rule_content_studio doesn't have tenant_id, so we query all rules
	// The tenant filtering happens in the onboarded_sources column which only shows sources for the requesting tenant
	// Note: id is selected internally for mapping but excluded from CSV output
	query := fmt.Sprintf(`
		SELECT 
			rcs.id,
			rcs.name,
			rcs.description,
			rcs.conditions,
			rcs.scope,
			rcs.action_type,
			rcs.type,
			rcs.sampling_rate,
			rcs.created_at,
			rcs.updated_at,
			rcs.source_vendor,
			rcs.source_device,
			rcs.source_log_type,
			rcs.release_number,
			rcs.reduction_percentage
		FROM rule_content_studio rcs
		WHERE %s`, whereClause)

	logging.GetLoggerWithContext(ctx).Info("query for content studio rules data", zap.String("query", query), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	return query, nil
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

	pageSize := utils.GetEnvInt("CONTENT_STUDIO_RULES_REPORT_PAGE_SIZE", 1000)
	offset := 0
	writeHeader := true

	// Get log source ID to name mapping for the tenant
	logSourceIdToNameMap, err := common.GetLogSourceIdToNamesMap(ctx, req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching log source names", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}

	// Get onboarded rules mapping: studio_rule_id -> []log_source_names
	onboardedRulesMap, err := getOnboardedRulesMap(ctx, req.TenantId, logSourceIdToNameMap)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching onboarded rules", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}

	for {
		writeHeader = writeHeader && offset == 0
		rows, columns, err := common.GetRowsAndColumnsByQueryWithJoins(query, pageSize, offset, "rcs.updated_at")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from rule_content_studio table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		if writeHeader {
			// Filter out id column from headers (keep it internally for mapping but don't show in CSV)
			var headerColumns []string
			for _, col := range columns {
				if col != "id" {
					headerColumns = append(headerColumns, col)
				}
			}
			// Add additional columns for onboarded status and source names
			headerColumns = append(headerColumns, "is_onboarded", "onboarded_sources")
			writer = csv.NewWriter(file)
			err = writer.Write(headerColumns)
			if err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
				return err
			}
		}
		fetchedRowsCount, err := writeRowsToFileForContentStudioRules(columns, rows, onboardedRulesMap, writer)
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

func writeRowsToFileForContentStudioRules(columns []string, rows *sql.Rows, onboardedRulesMap map[string][]string, writer *csv.Writer) (int, error) {
	fetchedRowsCount := 0
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(sql.RawBytes)
	}

	for rows.Next() {
		ruleId := ""
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}

		var row []string
		for i, col := range values {
			// Find the id column to get the rule ID (needed for mapping but excluded from output)
			if columns[i] == "id" {
				ruleId = string(*col.(*sql.RawBytes))
				continue // Skip id column in output
			}
			columnValue := string(*col.(*sql.RawBytes))
			row = append(row, columnValue)
		}

		// Check if rule is onboarded and get source names
		isOnboarded := "No"
		sourceNames := ""
		if sources, ok := onboardedRulesMap[ruleId]; ok && len(sources) > 0 {
			isOnboarded = "Yes"
			sourceNames = strings.Join(sources, "\n")
		}

		row = append(row, isOnboarded)
		row = append(row, sourceNames)

		err := writer.Write(row)
		if err != nil {
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

func getOnboardedRulesMap(ctx context.Context, tenantId string, logSourceIdToNameMap map[string]string) (map[string][]string, error) {
	// Query to get all onboarded content studio rules for the tenant
	// Join vc_rule with log_source to get the source names
	query := fmt.Sprintf(`
		SELECT DISTINCT
			vr.studio_rule_id,
			ls.id as log_source_id,
			ls.name as log_source_name
		FROM vc_rule vr
		INNER JOIN log_source ls ON vr.log_source_id = ls.id
		WHERE vr.studio_rule_id IS NOT NULL
		AND ls.tenant_id = '%s'::uuid
		ORDER BY vr.studio_rule_id
	`, tenantId)

	rows, err := config.GetDB().Raw(query).Rows()
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching onboarded rules", zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	onboardedRulesMap := make(map[string][]string)
	values := make([]interface{}, 3)
	for i := range values {
		values[i] = new(sql.RawBytes)
	}

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		studioRuleId := string(*values[0].(*sql.RawBytes))
		logSourceId := string(*values[1].(*sql.RawBytes))
		logSourceName := string(*values[2].(*sql.RawBytes))

		// Skip if studio_rule_id is empty
		if studioRuleId == "" {
			continue
		}

		// Use log source name from the query result, fallback to map if needed
		sourceName := logSourceName
		if sourceName == "" {
			if name, ok := logSourceIdToNameMap[logSourceId]; ok && name != "" {
				sourceName = name
			} else if logSourceId != "" {
				sourceName = logSourceId // Fallback to ID if name not found
			} else {
				continue // Skip if we don't have a valid source name or ID
			}
		}

		// Add source name to the list for this studio rule
		if _, exists := onboardedRulesMap[studioRuleId]; !exists {
			onboardedRulesMap[studioRuleId] = []string{}
		}
		onboardedRulesMap[studioRuleId] = append(onboardedRulesMap[studioRuleId], sourceName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return onboardedRulesMap, nil
}
