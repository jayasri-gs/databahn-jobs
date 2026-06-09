package format

import (
	"encoding/csv"
	"io"
)

type CSVEncoder struct {
	writer  *csv.Writer
	columns []string
}

func NewCSVEncoder(w io.Writer, delimiter string) *CSVEncoder {
	writer := csv.NewWriter(w)

	delimRune := ','
	if len(delimiter) > 0 {
		delimRune = rune(delimiter[0])
	}
	writer.Comma = delimRune

	return &CSVEncoder{
		writer: writer,
	}
}

func (e *CSVEncoder) Init(columns []string) error {
	e.columns = columns
	return nil
}

func (e *CSVEncoder) WriteHeader() error {
	return e.writer.Write(e.columns)
}

func (e *CSVEncoder) WriteRow(row []interface{}) error {
	strRow := make([]string, len(row))
	for i, v := range row {
		strRow[i] = FormatValue(v)
	}
	return e.writer.Write(strRow)
}

func (e *CSVEncoder) Finalize() error {
	e.writer.Flush()
	return e.writer.Error()
}

func (e *CSVEncoder) ContentType() string {
	return "text/csv"
}

func (e *CSVEncoder) FileExtension() string {
	return "csv"
}
