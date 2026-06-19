package format

import (
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
