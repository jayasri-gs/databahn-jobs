package agentDiscoveredNodesReport

import (
	"context"
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

func WriteAgentDiscoveredNodesReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing agent discovered nodes report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	query, err := getQueryForAgentDiscoveredNodesData(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, query, file)
	if err != nil {
		return err
	}
	return nil
}

// extractRuntimeStatusFilterValues extracts runtime_status filter values from report configuration
func extractRuntimeStatusFilterValues(reportConfiguration map[string]interface{}) []string {
	var runtimeStatusFilterValues []string

	extractFromFilters := func(filters map[string]interface{}) {
		if runtimeStatusFilter, exists := filters["runtime_status"]; exists {
			if statusArray, ok := runtimeStatusFilter.([]interface{}); ok {
				for _, v := range statusArray {
					if s, ok := v.(string); ok {
						runtimeStatusFilterValues = append(runtimeStatusFilterValues, s)
					}
				}
			}
		}
	}

	if andFilters, ok := reportConfiguration["and_filters"].(map[string]interface{}); ok {
		extractFromFilters(andFilters)
	}
	if orFilters, ok := reportConfiguration["or_filters"].(map[string]interface{}); ok {
		extractFromFilters(orFilters)
	}

	return runtimeStatusFilterValues
}

// findRuntimeStatusColumnIndex finds the index of the runtime_status column in the columns slice
func findRuntimeStatusColumnIndex(columns []string) int {
	for i, col := range columns {
		if col == "runtime_status" {
			return i
		}
	}
	return -1
}

// writePagedDataToFile writes paginated data to CSV file
func writePagedDataToFile(ctx context.Context, req models.AuditReport, query string, file *os.File, runtimeStatusFilterValues []string) error {
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

	for {
		rows, columns, err := common.GetRowsAndColumnsByQueryWithJoins(query, pageSize, offset, "adn.updated_at")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from agent_discovered_nodes table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		// Initialize writer and write header on first iteration
		if offset == 0 {
			writer = csv.NewWriter(file)
			if err := writer.Write(columns); err != nil {
				rows.Close() // Close rows on error
				logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
				return err
			}
		}

		runtimeStatusColumnIndex := findRuntimeStatusColumnIndex(columns)
		rowsFetchedFromDB, err := common.WriteRowsToFileForDbReportTypeWithoutTimeFiltersWithStatusFilter(columns, rows, writer, runtimeStatusColumnIndex, runtimeStatusFilterValues)
		if err != nil {
			rows.Close() // Close rows on error
			logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		// Use rowsFetchedFromDB (not rowsWritten) for pagination decision
		// This ensures we continue fetching even if many rows are filtered out
		shouldContinue := rowsFetchedFromDB >= pageSize
		rows.Close() // Always close rows after processing to prevent connection leaks
		if !shouldContinue {
			break
		}
		offset += pageSize
	}

	return nil
}

func getReportAndWriteToFile(ctx context.Context, req models.AuditReport, query string, file *os.File) error {
	// Parse the report configuration to check for runtime_status filters
	var reportConfiguration map[string]interface{}
	if err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration); err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}

	// Extract runtime_status filter values
	runtimeStatusFilterValues := extractRuntimeStatusFilterValues(reportConfiguration)

	// Write paginated data to file
	return writePagedDataToFile(ctx, req, query, file, runtimeStatusFilterValues)
}

func getQueryForAgentDiscoveredNodesData(ctx context.Context, req models.AuditReport) (string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", err
	}
	filterToDbColumnMap := map[string]string{
		"runtime_status":    "adn.runtime_status",
		"subscription_name": "adn.subscription_name",
		"last_error":        "adn.last_error",
		"tenant_id":         "adn.tenant_id",
	}

	// Extract time range filters BEFORE removing them from reportConfiguration
	timeRangeAndClauses, timeRangeOrClauses := buildTimeRangeFilters(reportConfiguration)

	// Remove runtime_status and time range filters from reportConfiguration to prevent them from being processed in WHERE clause
	// since we handle runtime_status filtering in memory and time range filters separately
	if andFilters, ok := reportConfiguration["and_filters"].(map[string]interface{}); ok {
		delete(andFilters, "runtime_status")
		delete(andFilters, "created_at")
		delete(andFilters, "updated_at")
		delete(andFilters, "heartbeat_at")
	}
	if orFilters, ok := reportConfiguration["or_filters"].(map[string]interface{}); ok {
		delete(orFilters, "runtime_status")
		delete(orFilters, "created_at")
		delete(orFilters, "updated_at")
		delete(orFilters, "heartbeat_at")
	}

	whereClause, _, _ := common.BuildQueryFromFilters(reportConfiguration, req.TenantId, filterToDbColumnMap)

	// Add time range filters for created_at, updated_at, and heartbeat_at
	// AND filters are added directly, OR filters are grouped together
	if len(timeRangeAndClauses) > 0 {
		whereClause = fmt.Sprintf("%s AND %s", whereClause, strings.Join(timeRangeAndClauses, " AND "))
	}
	if len(timeRangeOrClauses) > 0 {
		orGroup := fmt.Sprintf("(%s)", strings.Join(timeRangeOrClauses, " OR "))
		whereClause = fmt.Sprintf("%s AND %s", whereClause, orGroup)
	}

	// Build the complete query - selecting all columns from agent_discovered_nodes
	query := fmt.Sprintf(`
		SELECT 
			adn.id,
			adn.tenant_id,
			adn.agent_id,
			adn.node_name,
			adn.subscription_name,
			adn.runtime_status,
			adn.events_processed,
			adn.events_processing_time,
			adn.heartbeat_at,
			adn.last_error,
			adn.last_error_message,
			adn.channel_subscriptions::text as channel_subscriptions,
			adn.created_at,
			adn.updated_at,
			adn.customer_id
		FROM agent_discovered_nodes adn
		WHERE %s`, whereClause)

	logging.GetLoggerWithContext(ctx).Info("query for agent discovered nodes data", zap.String("query", query), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	return query, nil
}

// buildTimeRangeFilters extracts and builds SQL clauses for time range filters
// Supports: created_at, updated_at, heartbeat_at with values: 1d, 7d, 30d, 90d, 1y
// Returns: AND clauses and OR clauses separately
func buildTimeRangeFilters(reportConfiguration map[string]interface{}) ([]string, []string) {
	var timeRangeAndClauses []string
	var timeRangeOrClauses []string
	validRanges := map[string]string{
		"1d":  "1 day",
		"7d":  "7 days",
		"30d": "30 days",
		"90d": "90 days",
		"1y":  "1 year",
	}

	// Extract time range filters from and_filters
	if andFilters, ok := reportConfiguration["and_filters"].(map[string]interface{}); ok {
		// Handle created_at filter
		if createdAtFilter, exists := andFilters["created_at"]; exists {
			if rangeValue, ok := getSingleStringValue(createdAtFilter); ok {
				if interval, valid := validRanges[rangeValue]; valid {
					timeRangeAndClauses = append(timeRangeAndClauses, fmt.Sprintf("adn.created_at >= NOW() - INTERVAL '%s'", interval))
				}
			}
		}

		// Handle updated_at filter
		if updatedAtFilter, exists := andFilters["updated_at"]; exists {
			if rangeValue, ok := getSingleStringValue(updatedAtFilter); ok {
				if interval, valid := validRanges[rangeValue]; valid {
					timeRangeAndClauses = append(timeRangeAndClauses, fmt.Sprintf("adn.updated_at >= NOW() - INTERVAL '%s'", interval))
				}
			}
		}

		// Handle heartbeat_at filter
		if heartbeatAtFilter, exists := andFilters["heartbeat_at"]; exists {
			if rangeValue, ok := getSingleStringValue(heartbeatAtFilter); ok {
				if interval, valid := validRanges[rangeValue]; valid {
					timeRangeAndClauses = append(timeRangeAndClauses, fmt.Sprintf("adn.heartbeat_at >= NOW() - INTERVAL '%s'", interval))
				}
			}
		}
	}

	// Extract time range filters from or_filters
	if orFilters, ok := reportConfiguration["or_filters"].(map[string]interface{}); ok {
		// Handle created_at OR filter
		if createdAtFilter, exists := orFilters["created_at"]; exists {
			if rangeValue, ok := getSingleStringValue(createdAtFilter); ok {
				if interval, valid := validRanges[rangeValue]; valid {
					timeRangeOrClauses = append(timeRangeOrClauses, fmt.Sprintf("adn.created_at >= NOW() - INTERVAL '%s'", interval))
				}
			}
		}

		// Handle updated_at OR filter
		if updatedAtFilter, exists := orFilters["updated_at"]; exists {
			if rangeValue, ok := getSingleStringValue(updatedAtFilter); ok {
				if interval, valid := validRanges[rangeValue]; valid {
					timeRangeOrClauses = append(timeRangeOrClauses, fmt.Sprintf("adn.updated_at >= NOW() - INTERVAL '%s'", interval))
				}
			}
		}

		// Handle heartbeat_at OR filter
		if heartbeatAtFilter, exists := orFilters["heartbeat_at"]; exists {
			if rangeValue, ok := getSingleStringValue(heartbeatAtFilter); ok {
				if interval, valid := validRanges[rangeValue]; valid {
					timeRangeOrClauses = append(timeRangeOrClauses, fmt.Sprintf("adn.heartbeat_at >= NOW() - INTERVAL '%s'", interval))
				}
			}
		}
	}

	return timeRangeAndClauses, timeRangeOrClauses
}

// getSingleStringValue extracts a single string value from filter (handles both string and array with one element)
func getSingleStringValue(filterValue interface{}) (string, bool) {
	// If it's already a string, return it
	if str, ok := filterValue.(string); ok {
		return str, true
	}
	// If it's an array with one element
	if arr, ok := filterValue.([]interface{}); ok && len(arr) == 1 {
		if str, ok := arr[0].(string); ok {
			return str, true
		}
	}
	return "", false
}

func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, query string, file *os.File) error {
	err := getReportAndWriteToFile(ctx, req, query, file)
	if err != nil {
		return err
	}
	return nil
}
