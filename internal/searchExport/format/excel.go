package format

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/xuri/excelize/v2"
)

type ExcelEncoder struct {
	writer   io.Writer
	file     *excelize.File
	columns  []string
	rowNum   int
	sheetNum int
	curSheet string
}

const maxRowsPerSheet = 1000000

func NewExcelEncoder(w io.Writer) *ExcelEncoder {
	return &ExcelEncoder{
		writer:   w,
		sheetNum: 1,
	}
}

func (e *ExcelEncoder) Init(columns []string) error {
	e.columns = columns
	e.file = excelize.NewFile()
	e.curSheet = "Sheet1"
	return nil
}

func (e *ExcelEncoder) WriteHeader() error {
	for i, col := range e.columns {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		e.file.SetCellValue(e.curSheet, cell, col)
	}
	e.rowNum = 2
	return nil
}

func (e *ExcelEncoder) WriteRow(row []interface{}) error {
	if e.rowNum > maxRowsPerSheet {
		e.sheetNum++
		e.curSheet = fmt.Sprintf("Sheet%d", e.sheetNum)
		e.file.NewSheet(e.curSheet)
		for i, col := range e.columns {
			cell, _ := excelize.CoordinatesToCellName(i+1, 1)
			e.file.SetCellValue(e.curSheet, cell, col)
		}
		e.rowNum = 2
	}

	for i, val := range row {
		cell, _ := excelize.CoordinatesToCellName(i+1, e.rowNum)
		e.file.SetCellValue(e.curSheet, cell, excelCellValue(val))
	}
	e.rowNum++
	return nil
}

// maxExactExcelInt is the largest integer float64 represents exactly (2^53).
const maxExactExcelInt = 1 << 53

// isIntegerLiteral reports whether a JSON number was written without a fraction or exponent.
func isIntegerLiteral(text string) bool {
	if text == "" {
		return false
	}
	return !strings.ContainsAny(text, ".eE")
}

// excelCellValue maps the value types excelize does not recognise onto ones it does.
// json.RawMessage and json.Number are named types, so excelize's own type switch misses
// them and would render a `dynamic` column as a list of byte values.
func excelCellValue(v interface{}) interface{} {
	switch val := v.(type) {
	case json.RawMessage:
		return string(val)
	case json.Number:
		text := val.String()
		// Excel stores numbers as float64, so an integer beyond 2^53 cannot round-trip:
		// 9007199254740993 would land as ...992. Integer literals outside that range keep
		// their exact digits as text. Values written as decimals or in exponent notation were
		// never exact to begin with, so float is the intended representation.
		if isIntegerLiteral(text) {
			if i, err := val.Int64(); err == nil && i >= -maxExactExcelInt && i <= maxExactExcelInt {
				return i
			}
			return text
		}
		if f, err := val.Float64(); err == nil {
			return f
		}
		return text
	default:
		return v
	}
}

func (e *ExcelEncoder) Finalize() error {
	return e.file.Write(e.writer)
}

func (e *ExcelEncoder) ContentType() string {
	return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}

func (e *ExcelEncoder) FileExtension() string {
	return "xlsx"
}
