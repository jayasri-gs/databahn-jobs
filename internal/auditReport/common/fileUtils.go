package common

import (
	"encoding/csv"
	"os"
)

func CreateTempFile(fileName string) (*os.File, error) {
	f, err := os.Create(os.TempDir() + "/" + fileName + ".csv")
	if err != nil {
		return nil, err
	}
	return f, nil
}

func WriteRows(writer *csv.Writer, rows [][]string) error {
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}
