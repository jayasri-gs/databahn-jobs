package common

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"strconv"
	"strings"
)

func CreateTempFile(fileName string) (*os.File, error) {
	f, err := os.Create(os.TempDir() + "/" + fileName + ".csv")
	if err != nil {
		return nil, err
	}
	return f, nil
}

func UpdateQueryFromFilter(configData map[string]interface{}, otherParamsAdded bool, query string, filterKey string, dbField string) (bool, string) {
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
func ConvertToStrings(input []interface{}) ([]string, error) {
	var result []string
	for _, v := range input {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("element %v is not a string", v)
		}
		result = append(result, str)
	}
	return result, nil
}
func ValidateConfig(startTime string, endTime string) error {

	if startTime == "" || endTime == "" {
		return errors.New("start time and end time cannot be empty")
	}
	// Parse time strings into time.Time objects
	start, err := strconv.Atoi(startTime)
	if err != nil {
		return err
	}

	end, err := strconv.Atoi(endTime)
	if err != nil {
		return err
	}

	// Compare time1 and time2
	if start > end {
		return errors.New("startTime greater than endTime")
	}
	return nil
}
func WriteRowToTheFileOneByOne(columns []string, rows *sql.Rows, writer *csv.Writer, fetchedRowsCount int) (int, error) {
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}

		var row []string
		for _, col := range values {
			// Convert column value to string
			columnValue := string(*col.(*sql.RawBytes))
			row = append(row, columnValue)
		}
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
func GetLogSourceIdToNamesMap(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests) (*[]models.FailedRequests, map[string]string, error) {
	logSources, err := helper.GetAllLogSourcesByTenantId(ctx, config.GetDB(), req.TenantId)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching logsources", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}
	logsourceIdToNameMap := make(map[string]string)
	for _, ls := range logSources {
		logsourceIdToNameMap[ls.ID.String()] = ls.Name
	}
	return failedRequests, logsourceIdToNameMap, nil
}

func GetDestinationIdToNamesMap(ctx context.Context, req models.AuditReport, failedRequests *[]models.FailedRequests) (*[]models.FailedRequests, map[string]string, error) {
	destinations, err := destination.GetDestinationByTenantId(utils.UUIDFromStringOrNil(req.TenantId), config.GetDB())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching destinations", zap.Error(err))
		errRequest := models.NewFailedRequest(req.Id.String(), req.Name, req.TenantId, req.Retries+1, err.Error())
		*failedRequests = append(*failedRequests, errRequest)
		return nil, nil, err
	}
	destinationIdToNameMap := make(map[string]string)
	for _, dest := range destinations {
		destinationIdToNameMap[dest.ID.String()] = dest.Name
	}
	destinationIdToNameMap["dbd00000-0000-0000-0000-000000000000"] = "Databahn Sandbox"
	return failedRequests, destinationIdToNameMap, nil
}
