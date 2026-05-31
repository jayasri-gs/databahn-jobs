package processor

import (
	"fmt"
	"os"
	"strings"

	"github.com/databahn-ai/go-logging/logger"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"go.uber.org/zap"
)

// ParquetScanner implements a scanner-like interface for parquet files.
// If the file footer is missing (truncated download), it falls back to SalvageParquetFile.
type ParquetScanner struct {
	reader          *reader.ParquetReader
	data            []byte
	err             error
	currentRow      int64
	forwardDataType string

	salvageMode     bool
	salvageMessages []string
	salvageIndex    int
}

type ParsedParquetRow struct {
	Rawevent *string `parquet:"name=rawevent, type=BYTE_ARRAY, convertedtype=UTF8" json:"rawevent"`
}

type RawParquetRow struct {
	Message *string `json:"message" parquet:"name=message, type=BYTE_ARRAY,convertedtype=UTF8, encoding=PLAIN"`
}

// NewParquetScanner creates a scanner for parquet files (normal reader, salvage fallback on truncated files).
func NewParquetScanner(f *os.File, forwardDataType string) (*ParquetScanner, error) {
	r, err := getParquetReader(f, forwardDataType)
	if err == nil {
		return &ParquetScanner{
			reader:          r,
			forwardDataType: forwardDataType,
		}, nil
	}
	if !isTruncatedParquetError(err) {
		return nil, err
	}

	data, readErr := os.ReadFile(f.Name())
	if readErr != nil {
		return nil, fmt.Errorf("truncated parquet %s: %w (re-read: %v)", f.Name(), err, readErr)
	}

	messages, salvErr := SalvageParquetFile(data)
	if salvErr != nil {
		return nil, fmt.Errorf("truncated parquet %s: %w (salvage: %v)", f.Name(), err, salvErr)
	}

	logger.GetLogger().Warn(
		"parquet file missing footer; replaying salvaged rows only",
		zap.String("file", f.Name()),
		zap.Int("salvagedRows", len(messages)),
		zap.Error(err),
	)

	return &ParquetScanner{
		salvageMode:     true,
		salvageMessages: messages,
		forwardDataType: forwardDataType,
	}, nil
}

// Salvaged reports whether the scanner is using the truncated-file salvage path.
func (s *ParquetScanner) Salvaged() bool {
	return s.salvageMode
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

// Scan advances the scanner to the next non-empty row.
func (s *ParquetScanner) Scan() bool {
	if s.salvageMode {
		for s.salvageIndex < len(s.salvageMessages) {
			msg := s.salvageMessages[s.salvageIndex]
			s.salvageIndex++
			if len(msg) == 0 {
				continue
			}
			s.data = []byte(msg)
			return true
		}
		return false
	}

	for s.Advance() {
		if len(s.data) > 0 {
			return true
		}
	}
	return false
}

// NumRows returns the total number of rows in the parquet file.
func (s *ParquetScanner) NumRows() int64 {
	if s.salvageMode {
		return int64(len(s.salvageMessages))
	}
	return s.reader.GetNumRows()
}

// Advance moves to the next row, including rows with empty messages.
func (s *ParquetScanner) Advance() bool {
	if s.salvageMode {
		if s.salvageIndex >= len(s.salvageMessages) {
			return false
		}
		s.data = []byte(s.salvageMessages[s.salvageIndex])
		s.salvageIndex++
		s.currentRow++
		return true
	}

	if s.currentRow >= s.reader.GetNumRows() {
		return false
	}

	rowData, err := s.getRowData(s.forwardDataType)
	if err != nil {
		s.err = err
		return false
	}
	s.currentRow++
	s.data = []byte(rowData)
	return true
}

func (s *ParquetScanner) getRowData(forwardDataType string) (string, error) {
	if strings.EqualFold(forwardDataType, "parsed") {
		rows := make([]*ParsedParquetRow, 1)
		if err := s.reader.Read(&rows); err != nil {
			return "", err
		}
		if len(rows) == 0 || rows[0] == nil || rows[0].Rawevent == nil {
			return "", nil
		}
		return *rows[0].Rawevent, nil
	}

	rows := make([]*RawParquetRow, 1)
	if err := s.reader.Read(&rows); err != nil {
		return "", err
	}
	if len(rows) == 0 || rows[0] == nil || rows[0].Message == nil {
		return "", nil
	}
	return *rows[0].Message, nil
}

// Text returns the current row as a string.
func (s *ParquetScanner) Text() string {
	return string(s.data)
}

// Err returns any error that occurred during scanning.
func (s *ParquetScanner) Err() error {
	return s.err
}

// Close closes the underlying parquet reader.
func (s *ParquetScanner) Close() error {
	if s.salvageMode || s.reader == nil {
		return nil
	}
	s.reader.ReadStop()
	return nil
}
