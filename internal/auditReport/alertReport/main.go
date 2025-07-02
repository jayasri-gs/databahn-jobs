package alertReport

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"strconv"
	"strings"
)

func WriteAlertReportToFile(ctx context.Context, req models.AuditReport, file *os.File) error {
	logging.GetLoggerWithContext(ctx).Info("writing alert report to file", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
	query, err := getAlertReportConfigFromRequest(ctx, req)
	if err != nil {
		return err
	}
	err = gatherDataAndWriteToFile(ctx, req, query, file)
	if err != nil {
		return err
	}
	return nil
}
func getAlertReportConfigFromRequest(ctx context.Context, req models.AuditReport) (string, error) {
	var alertReportConfiguration map[string]interface{}
	var startTime, endTime string
	err := json.Unmarshal(req.AuditReportFilter, &alertReportConfiguration)
	if err != nil {
		return "", err
	}
	var timeFilters, orFilters map[string]interface{}
	timeFilters = alertReportConfiguration["time_filters"].(map[string]interface{})
	orFilters = alertReportConfiguration["or_filters"].(map[string]interface{})
	if _, ok := timeFilters["startTime"].(string); ok {
		startTime = timeFilters["startTime"].(string)
	} else {
		logging.GetLoggerWithContext(ctx).Error("startTime not found in config data or is a invalid string please check your config", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", fmt.Errorf("startTime not found in config data or is a invalid string please check your config")
	}
	if _, ok := timeFilters["endTime"].(string); ok {
		endTime = timeFilters["endTime"].(string)
	} else {
		logging.GetLoggerWithContext(ctx).Error("endTime not found in config data or is a invalid string please check your config", zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return "", fmt.Errorf("endTime not found in config data or is a invalid string please check your config")
	}
	dismissed, ok := orFilters["dismissed"].(bool)
	if !ok {
		dismissed = false
	}

	query := ""
	if dismissed {
		query = `updatedAt:>` + startTime + ` AND updatedAt:<` + endTime + ` AND tenantId: "` + req.TenantId + `"`
	} else {
		query = `updatedAt:>` + startTime + ` AND updatedAt:<` + endTime + ` AND tenantId: "` + req.TenantId + `"` + ` AND dismissed: false`

	}
	otherParamsAdded := false
	filterMappings := map[string]string{
		"status":        "status",
		"category":      "functionalityType",
		"severity":      "criticality",
		"functionality": "functionality",
	}
	// write a function to update other fields in the query if present in the config obj
	for k, v := range filterMappings {
		value, ok := orFilters[k]
		if !ok || len(value.([]interface{})) == 0 {
			logging.GetLogger().Info(fmt.Sprintf("no config not found for filter %s, ignoring this filter criteria", k), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			continue
		}
		if !otherParamsAdded {
			query += " AND "
			otherParamsAdded = true
		} else {
			query += " AND "
		}
		if sliceValue, ok := value.([]interface{}); ok {
			// Handle slice values
			stringSlice, err := convertInterfaceSliceToStringSlice(sliceValue)
			if err != nil {
				return "", err
			}
			if len(stringSlice) == 0 {
				continue
			}
			query += fmt.Sprintf("%s: (\"%s\")", v, strings.Join(stringSlice, "\" OR \""))
		} else {
			logging.GetLoggerWithContext(ctx).Error(fmt.Sprintf("invalid type for %s in config data", k), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
			return "", fmt.Errorf("invalid type for %s in config data", k)
		}
	}

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
	alertResponse, err := getAlertsFromOpenSearch(ctx, query)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting alert stats", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	// Write headers to the file
	headers := []string{"Id", "Title", "Criticality", "Message", "CreatedAt", "UpdatedAt", "FirstObservedAt", "LastObservedAt", "TenantId", "FunctionalityType", "Functionality", "FunctionalityEntityId", "FunctionalityEntityName", "Dismissed", "DismissedAt", "DismissedBy"}
	writer = csv.NewWriter(file)
	err = writer.Write(headers)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing headers to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	err = writeAlertRowsToFile(alertResponse, writer)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while writing rows to the file", zap.Error(err), zap.String("request_id", req.Id.String()), zap.String("report_name", req.Name), zap.String("tenant_id", req.TenantId))
		return err
	}
	return nil
}
func writeAlertRowsToFile(alertResponse []statistics.AlertDocument, writer *csv.Writer) error {
	for _, alert := range alertResponse {
		row := []string{
			alert.Id,
			alert.Title,
			alert.Criticality,
			alert.Message,
			strconv.FormatInt(alert.CreatedAt, 10),
			strconv.FormatInt(alert.UpdatedAt, 10),
			strconv.FormatInt(alert.FirstObservedAt, 10),
			strconv.FormatInt(alert.LastObservedAt, 10),
			alert.TenantId,
			alert.FunctionalityType,
			alert.Functionality,
			alert.FunctionalityEntityId,
			alert.FunctionalityEntityName,
			strconv.FormatBool(alert.Dismissed),
			strconv.FormatInt(alert.DismissedAt, 10),
			alert.DismissedBy,
		}
		err := writer.Write(row)
		if err != nil {
			logging.GetLogger().Error("error while writing row to the file", zap.Error(err))
			return err
		}
	}
	logging.GetLogger().Info("Writing rows to the file completed")
	writer.Flush()
	return writer.Error()
}
func convertInterfaceSliceToStringSlice(interfaceSlice []interface{}) ([]string, error) {
	stringSlice := make([]string, len(interfaceSlice))
	for i, v := range interfaceSlice {
		stringSlice[i] = fmt.Sprintf("%v", v)
	}
	return stringSlice, nil
}
