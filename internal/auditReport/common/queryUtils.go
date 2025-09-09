package common

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	logging "github.com/databahn-ai/go-logging/logger"
)

func GetDbQueryWithoutTimeFilters(req models.AuditReport, filterToDbColumnMap map[string]string) (string, error) {
	var reportConfiguration map[string]interface{}
	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		return "", err
	}
	var configData map[string]interface{}
	configData = reportConfiguration["filter"].(map[string]interface{})

	query := fmt.Sprintf("tenant_id = '%s'", req.TenantId)

	otherParamsAdded := false
	for filterKey, dbField := range filterToDbColumnMap {
		otherParamsAdded, query = updateQueryFromFilterForDbFetch(configData, otherParamsAdded, query, filterKey, dbField)
	}
	if otherParamsAdded {
		query += ")"
	}
	return query, nil
}
func GetDbQueryWithTimeFilters(req models.AuditReport, filterToDbColumnMap map[string]string) (string, string, string, error) {
	var reportConfiguration map[string]interface{}

	err := json.Unmarshal(req.AuditReportFilter, &reportConfiguration)
	if err != nil {
		return "", "", "", err
	}
	var configData map[string]interface{}
	configData = reportConfiguration["filter"].(map[string]interface{})

	startTime, ok := configData["startTime"].(string)
	if !ok || startTime == "" {
		return "", "", "", fmt.Errorf("startTime missing or not a string in config")
	}
	endTime, ok := configData["endTime"].(string)
	if !ok || endTime == "" {
		return "", "", "", fmt.Errorf("endTime missing or not a string in config")
	}

	err = ValidateConfig(startTime, endTime)
	if err != nil {
		return "", "", "", err
	}

	query := fmt.Sprintf("tenant_id = '%s'", req.TenantId)

	otherParamsAdded := false
	for filterKey, dbField := range filterToDbColumnMap {
		otherParamsAdded, query = updateQueryFromFilterForDbFetch(configData, otherParamsAdded, query, filterKey, dbField)
	}
	if otherParamsAdded {
		query += ")"
	}
	return startTime, endTime, query, nil
}
func updateQueryFromFilterForDbFetch(configData map[string]interface{}, otherParamsAdded bool, query string, filterKey string, dbField string) (bool, string) {
	fieldKey, ok := configData[filterKey]
	if !ok || len(fieldKey.([]interface{})) == 0 {
		logging.GetLogger().Info(fmt.Sprintf("no config not found for filter %s, ignoring this filter criteria", filterKey))
	} else {
		if !otherParamsAdded {
			query += " and ("
			otherParamsAdded = true
		} else {
			query += " or "
		}
		fieldsArray, _ := ConvertToStrings(fieldKey.([]interface{}))
		query += fmt.Sprintf("%s in ('%s')", dbField, strings.Join(fieldsArray, "','"))
	}
	return otherParamsAdded, query
}

func BuildQuery(configData map[string]interface{}, filterMappings map[string]string, baseQuery string) (string, error) {
	query := baseQuery
	otherParamsAdded := false

	for k, v := range filterMappings {
		if value, ok := configData[k]; ok {
			if !otherParamsAdded {
				query += " AND "
				otherParamsAdded = true
			} else {
				query += " AND "
			}

			if strValue, ok := value.(string); ok {
				query += fmt.Sprintf("%s: \"%s\"", v, strValue)
			} else if sliceValue, ok := value.([]interface{}); ok {
				stringSlice := make([]string, len(sliceValue))
				for i, v := range sliceValue {
					stringSlice[i] = fmt.Sprintf("\"%s\"", v)
				}
				query += fmt.Sprintf("%s: (%s)", v, strings.Join(stringSlice, " OR "))
			} else {
				return "", fmt.Errorf("invalid type for %s in config data", k)
			}
		}
	}

	return query, nil
}

// BuildQueryFromFilters builds a query string from the new filter structure (and_filters, or_filters, time_filters)
// filterToDbColumnMap maps filter keys to DB column names
// Returns: query string (without time filter), startTime, endTime
func BuildQueryFromFilters(filters map[string]interface{}, tenantId string, filterToDbColumnMap map[string]string) (string, string, string) {
	// Use the mapped column name for tenant_id, fallback to "tenant_id" if not mapped
	tenantColumn := "tenant_id"
	if mappedColumn, ok := filterToDbColumnMap["tenant_id"]; ok {
		tenantColumn = mappedColumn
	}
	queryParts := []string{fmt.Sprintf("%s = '%s'", tenantColumn, tenantId)}
	var startTime, endTime string

	// Handle time_filters (optional)
	if tf, ok := filters["time_filters"].(map[string]interface{}); ok {
		if v, ok := tf["startTime"].(string); ok && v != "" {
			startTime = v
		}
		if v, ok := tf["endTime"].(string); ok && v != "" {
			endTime = v
		}
	}

	// Handle and_filters
	if andFilters, ok := filters["and_filters"].(map[string]interface{}); ok {
		for key, val := range andFilters {
			dbCol, ok := filterToDbColumnMap[key]
			if !ok {
				dbCol = key
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

	// Handle or_filters (now as IN clauses per key)
	orGroup := []string{}
	if orFilters, ok := filters["or_filters"].(map[string]interface{}); ok {
		for key, val := range orFilters {
			dbCol, ok := filterToDbColumnMap[key]
			if !ok {
				dbCol = key
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

	// Do NOT add time_filters to the query string

	finalQuery := strings.Join(queryParts, " AND ")
	return finalQuery, startTime, endTime
}
