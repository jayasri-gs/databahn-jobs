package format

import (
	"encoding/json"
	"io"
)

type JSONEncoder struct {
	writer  io.Writer
	columns []string
	encoder *json.Encoder
}

func NewJSONEncoder(w io.Writer) *JSONEncoder {
	return &JSONEncoder{
		writer:  w,
		encoder: json.NewEncoder(w),
	}
}

func (e *JSONEncoder) Init(columns []string) error {
	e.columns = columns
	return nil
}

func (e *JSONEncoder) WriteHeader() error {
	return nil
}

func (e *JSONEncoder) WriteRow(row []interface{}) error {
	record := make(map[string]interface{})
	for i, col := range e.columns {
		if i < len(row) {
			record[col] = row[i]
		}
	}
	return e.encoder.Encode(record)
}

func (e *JSONEncoder) Finalize() error {
	return nil
}

func (e *JSONEncoder) ContentType() string {
	return "application/x-ndjson"
}

func (e *JSONEncoder) FileExtension() string {
	return "json"
}
