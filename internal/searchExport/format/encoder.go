package format

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type FormatEncoder interface {
	Init(columns []string) error
	WriteHeader() error
	WriteRow(row []interface{}) error
	Finalize() error
	ContentType() string
	FileExtension() string
}

func NewEncoder(format string, writer io.Writer, delimiter string) (FormatEncoder, error) {
	switch format {
	case "csv":
		return NewCSVEncoder(writer, delimiter), nil
	case "json":
		return NewJSONEncoder(writer), nil
	case "xlsx", "excel":
		return NewExcelEncoder(writer), nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

func FormatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	// json.RawMessage carries a KQL `dynamic` column verbatim. It must be matched before
	// []byte: a type switch compares exact types, so the []byte case below never sees it,
	// and the default would render it as a list of byte values.
	case json.RawMessage:
		return string(val)
	case json.Number:
		return val.String()
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}
