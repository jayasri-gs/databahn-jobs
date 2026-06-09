package format

import (
	"fmt"
	"io"

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
		e.file.SetCellValue(e.curSheet, cell, val)
	}
	e.rowNum++
	return nil
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
