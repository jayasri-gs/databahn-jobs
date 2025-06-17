package common

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"strings"
	"time"
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
		logging.GetLogger().Info("no types found in the config, ignoring this filter criteria")
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
	start, err := time.Parse(time.RFC3339, startTime)
	if err != nil {
		return err
	}

	end, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return err
	}

	// Compare time1 and time2
	if start.After(end) {
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
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return fetchedRowsCount, nil
}
