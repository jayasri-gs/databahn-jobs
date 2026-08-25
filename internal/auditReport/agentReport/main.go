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

const (
	groupingLevelAgent    = "AGENT"
	groupingLevelAgentTag = "AGENT_TAG"
)

type agentReportConfiguration struct {
	AgentReportConfig *struct {
		GroupingLevel string `json:"groupingLevel"`
	} `json:"agentReportConfig"`
}

func WriteAgentReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing agent report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	query, err := getQueryForAgentData(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, query, file)
	if err != nil {
		return err
	}
	return nil
}

// extractStatusFilterValues extracts status filter values from report configuration
func extractStatusFilterValues(reportConfiguration map[string]interface{}) []string {
	var statusFilterValues []string

	extractFromFilters := func(filters map[string]interface{}) {
		if statusFilter, exists := filters["status"]; exists {
			if statusArray, ok := statusFilter.([]interface{}); ok {
				for _, v := range statusArray {
					if s, ok := v.(string); ok {
						statusFilterValues = append(statusFilterValues, s)
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

	return statusFilterValues
}

// findStatusColumnIndex finds the index of the status column in the columns slice
func findStatusColumnIndex(columns []string) int {
	for i, col := range columns {
		if col == "status" {
			return i
		}
	}
	return -1
}

// writePagedDataToFile writes paginated data to CSV file
func writePagedDataToFile(ctx context.Context, req models.AuditReport, query string, file *os.File, statusFilterValues []string) error {
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
		rows, columns, err := common.GetRowsAndColumnsByQueryWithJoins(query, pageSize, offset, "a.updated_at")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while fetching data from agent table", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		// Initialize writer and write header on first iteration
		if offset == 0 {
			writer = csv.NewWriter(file)
			if err := writer.Write(columns); err != nil {
				logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
				return err
			}
		}

		statusColumnIndex := findStatusColumnIndex(columns)
		fetchedRowsCount, err := common.WriteRowsToFileForDbReportTypeWithoutTimeFiltersWithStatusFilter(columns, rows, writer, statusColumnIndex, statusFilterValues)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return err
		}

		if fetchedRowsCount < pageSize {
			break
		}
		offset += pageSize
	}

	return nil
}

func getReportAndWriteToFile(ctx context.Context, req models.AuditReport, query string, file *os.File) error {
	// Parse the report configuration to check for status filters
	var reportConfiguration map[string]interface{}
	if err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration); err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}

	// Extract status filter values
	statusFilterValues := extractStatusFilterValues(reportConfiguration)

	// Write paginated data to file
	return writePagedDataToFile(ctx, req, query, file, statusFilterValues)
}
func getQueryForAgentData(ctx context.Context, req models.AuditReport) (string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while unmarshalling report filter", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", err
	}
	filterToDbColumnMap := map[string]string{
		"os":            "a.os",
		"cpuArch":       "a.cpu_arch",
		"platform":      "a.platform",
		"kernelArch":    "a.kernel_arch",
		"kernelVersion": "a.kernel_version",
		"tenant_id":     "a.tenant_id",
	}

	// Remove status from reportConfiguration to prevent it from being processed in WHERE clause
	// since we handle status filtering in memory
	if andFilters, ok := reportConfiguration["and_filters"].(map[string]interface{}); ok {
		delete(andFilters, "status")
	}
	if orFilters, ok := reportConfiguration["or_filters"].(map[string]interface{}); ok {
		delete(orFilters, "status")
	}

	whereClause, _, _ := common.BuildQueryFromFilters(reportConfiguration, req.TenantId, filterToDbColumnMap)

	groupingLevel := getGroupingLevel(req)
	tagColumns, groupByClause := agentTagColumnsAndGroupBy(groupingLevel)
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
			uu.email as updated_by,
			%s
		FROM agent_node a
		    LEFT JOIN fleet f on a.fleet_id = f.id
		LEFT JOIN data_planes dp ON a.data_plane_id = dp.id
		LEFT JOIN users uc ON a.created_by = uc.id
		LEFT JOIN users uu ON a.updated_by = uu.id
		LEFT JOIN agent_tag_mapping atm ON atm.agent_id = a.id AND atm.tenant_id = a.tenant_id
		LEFT JOIN tag t ON t.id = atm.tag_id AND t.tenant_id = a.tenant_id
		LEFT JOIN collection_profile_tag_mapping cptm ON cptm.tag_id = t.id AND cptm.tenant_id = a.tenant_id
		LEFT JOIN collection_profile cp ON cp.id = cptm.collection_profile_id AND cp.tenant_id = a.tenant_id
		WHERE %s
		%s`, tagColumns, whereClause, groupByClause)

	logging.GetLoggerWithContext(ctx).Info("query for agent data", zap.String("query", query), zap.String("grouping_level", groupingLevel), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	return query, nil
}

func agentTagColumnsAndGroupBy(groupingLevel string) (string, string) {
	if groupingLevel == groupingLevelAgentTag {
		return `t.name as tag_name,
			cp.name as collection_profile`, ""
	}
	return `STRING_AGG(DISTINCT t.name, ', ' ORDER BY t.name) as tags,
			COUNT(DISTINCT t.id) as tags_count,
			STRING_AGG(DISTINCT cp.name, ', ' ORDER BY cp.name) as collection_profile`,
		"GROUP BY a.id, dp.name, uc.email, uu.email"
}

func getGroupingLevel(req models.AuditReport) string {
	if len(req.ReportConfiguration) == 0 {
		return groupingLevelAgentTag
	}
	var config agentReportConfiguration
	if err := json.Unmarshal(req.ReportConfiguration, &config); err != nil {
		return groupingLevelAgentTag
	}
	if config.AgentReportConfig == nil || config.AgentReportConfig.GroupingLevel == "" {
		return groupingLevelAgentTag
	}
	return config.AgentReportConfig.GroupingLevel
}

func gatherDataAndWriteToFile(ctx context.Context, req models.AuditReport, query string, file *os.File) error {
	err := getReportAndWriteToFile(ctx, req, query, file)
	if err != nil {
		return err
	}
	return nil
}
