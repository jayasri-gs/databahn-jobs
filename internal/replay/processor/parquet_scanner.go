package processor

import (
	"os"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// ParquetScanner implements a scanner-like interface for parquet files
type ParquetScanner struct {
	reader          *reader.ParquetReader
	data            []byte
	err             error
	currentRow      int64
	forwardDataType string
}

type ParsedParquetRow struct {
	Rawevent string `parquet:"name=rawevent, type=BYTE_ARRAY, convertedtype=UTF8" json:"rawevent"`
}

type RawParquetRow struct {
	Message string `json:"message" parquet:"name=message, type=BYTE_ARRAY,convertedtype=UTF8, encoding=PLAIN"`
}

// NewParquetScanner creates a new scanner for parquet files
func NewParquetScanner(f *os.File, forwardDataType string) (*ParquetScanner, error) {
	r, err := getParquetReader(f, forwardDataType)
	if err != nil {
		return nil, err
	}
	return &ParquetScanner{
		reader:          r,
		currentRow:      0,
		forwardDataType: forwardDataType,
	}, nil
}

func getParquetReader(f *os.File, forwardDataType string) (*reader.ParquetReader, error) {
	fr, err := local.NewLocalFileReader(f.Name())
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(forwardDataType, "parsed") {
		return reader.NewParquetReader(fr, new(ParsedParquetRow), 4)
	}
	return reader.NewParquetReader(fr, new(RawParquetRow), 4)
}

// Scan advances the scanner to the next row
func (s *ParquetScanner) Scan() bool {
	if s.currentRow >= s.reader.GetNumRows() {
		return false
	}

	// Create a slice to hold one row
	rowData, err := s.getRowData(s.forwardDataType)
	if err != nil {
		s.err = err
		return false
	}
	s.currentRow++

	if rowData != "" {
		s.data = []byte(rowData)
		return true
	}

	return false
}

func (s *ParquetScanner) getRowData(forwardDataType string) (string, error) {
	if strings.EqualFold(forwardDataType, "parsed") {
		rows := make([]*ParsedParquetRow, 1)
		if err := s.reader.Read(&rows); err != nil {
			return "", err
		}
		return rows[0].Rawevent, nil
	}

	rows := make([]*RawParquetRow, 1)
	if err := s.reader.Read(&rows); err != nil {
		return "", err
	}
	return rows[0].Message, nil
}

// Text returns the current row as a JSON string
func (s *ParquetScanner) Text() string {
	return string(s.data)
}

// Err returns any error that occurred during scanning
func (s *ParquetScanner) Err() error {
	return s.err
}

// Close closes the underlying parquet reader
func (s *ParquetScanner) Close() error {
	s.reader.ReadStop()
	return nil
}
