package common

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
)

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
func WriteRowsToFileForDbReportTypeWithoutTimeFilters(columns []string, rows *sql.Rows, writer *csv.Writer) (int, error) {
	fetchedRowsCount := 0
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
