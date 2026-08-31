package query

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// The Sentinel lake KQL API answers in the Kusto v2 format: a top-level array of frames.
//
//	[{"FrameType":"DataSetHeader",...},
//	 {"FrameType":"DataTable","TableKind":"QueryProperties","Columns":[...],"Rows":[...]},
//	 {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":..,"ColumnType":..}],"Rows":[[..]]},
//	 {"FrameType":"DataSetCompletion","HasErrors":false}]
//
// Only the PrimaryResult frame carries export data; the others are query statistics. A
// DataSetCompletion frame with HasErrors reports a failure that arrived with HTTP 200, the
// same trap the analytics tier sets with partial results.
//
// Some deployments answer with the v1 object shape instead ({"Tables":[...]}), which is why
// backend-service's KustoJsonResponseParser accepts both. decodeSentinelLakeResponse does the
// same rather than assuming one.

const (
	lakeFrameDataTable       = "DataTable"
	lakeFrameDataSetComplete = "DataSetCompletion"
	lakeTableKindPrimary     = "PrimaryResult"
)

// lakeColumn is the v2 (and Kusto v1) column descriptor. Log Analytics uses lower-case
// "name"/"type"; Kusto uses "ColumnName"/"ColumnType", with "DataType" on older payloads.
type lakeColumn struct {
	ColumnName string `json:"ColumnName"`
	ColumnType string `json:"ColumnType"`
	DataType   string `json:"DataType"`
	Name       string `json:"name"`
	Type       string `json:"type"`
}

func (c lakeColumn) name() string {
	if c.ColumnName != "" {
		return c.ColumnName
	}
	return c.Name
}

// decodeSentinelLakeResponse streams a lake KQL response, invoking onColumns once before the
// first row. Rows are emitted one at a time so an export-sized result is never materialised.
func decodeSentinelLakeResponse(r io.Reader, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return 0, fmt.Errorf("parse Sentinel lake response: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return 0, fmt.Errorf("parse Sentinel lake response: unexpected token %v", tok)
	}

	switch delim {
	case '[':
		return decodeLakeFrames(dec, onColumns, onRow)
	case '{':
		return decodeLakeObject(dec, onColumns, onRow)
	default:
		return 0, fmt.Errorf("parse Sentinel lake response: unexpected delimiter %q", delim)
	}
}

// decodeLakeFrames walks the v2 frame array. The opening '[' has already been consumed.
func decodeLakeFrames(dec *json.Decoder, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	var rows int64
	emitted := false

	for dec.More() {
		n, failure, err := decodeLakeFrame(dec, &emitted, onColumns, onRow)
		rows += n
		if err != nil {
			return rows, err
		}
		if failure != "" {
			return rows, fmt.Errorf("Sentinel lake query failed: %s", failure)
		}
	}
	if err := expectDelim(dec, ']'); err != nil {
		return rows, fmt.Errorf("parse Sentinel lake response: %w", err)
	}
	return rows, nil
}

// decodeLakeFrame handles one frame, returning the rows it emitted and any failure the frame
// reported. emitted guards against a second data frame overwriting the export's columns.
func decodeLakeFrame(dec *json.Decoder, emitted *bool, onColumns func([]string) error, onRow func([]interface{}) error) (int64, string, error) {
	if err := expectDelim(dec, '{'); err != nil {
		return 0, "", fmt.Errorf("parse Sentinel lake frame: %w", err)
	}

	var (
		frameType string
		tableKind string
		columns   []string
		rows      int64
		hasErrors bool
		errDetail string
	)

	for dec.More() {
		key, err := objectKey(dec)
		if err != nil {
			return rows, "", fmt.Errorf("parse Sentinel lake frame: %w", err)
		}
		switch key {
		case "FrameType":
			if err := dec.Decode(&frameType); err != nil {
				return rows, "", fmt.Errorf("parse Sentinel lake frame type: %w", err)
			}
		case "TableKind":
			if err := dec.Decode(&tableKind); err != nil {
				return rows, "", fmt.Errorf("parse Sentinel lake table kind: %w", err)
			}
		case "Columns", "columns":
			cols, err := decodeLakeColumns(dec)
			if err != nil {
				return rows, "", err
			}
			columns = cols
		case "Rows", "rows":
			n, err := decodeLakeRows(dec, isLakePrimary(frameType, tableKind), emitted, columns, onColumns, onRow)
			rows += n
			if err != nil {
				return rows, "", err
			}
		case "HasErrors":
			if err := dec.Decode(&hasErrors); err != nil {
				return rows, "", fmt.Errorf("parse Sentinel lake completion: %w", err)
			}
		case "OneApiErrors", "Exceptions", "error":
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return rows, "", fmt.Errorf("parse Sentinel lake error detail: %w", err)
			}
			errDetail = strings.TrimSpace(string(raw))
		default:
			if err := skipValue(dec); err != nil {
				return rows, "", fmt.Errorf("parse Sentinel lake frame: %w", err)
			}
		}
	}

	if err := expectDelim(dec, '}'); err != nil {
		return rows, "", fmt.Errorf("parse Sentinel lake frame: %w", err)
	}

	if frameType == lakeFrameDataSetComplete && hasErrors {
		if errDetail == "" || errDetail == "null" {
			errDetail = "the service reported errors without detail"
		}
		return rows, errDetail, nil
	}
	return rows, "", nil
}

// isLakePrimary reports whether a frame carries export data. A DataTable with no TableKind is
// treated as primary: the v1 object shape omits it, and metadata tables always name theirs.
func isLakePrimary(frameType, tableKind string) bool {
	if frameType != "" && frameType != lakeFrameDataTable {
		return false
	}
	return tableKind == "" || tableKind == lakeTableKindPrimary
}

func decodeLakeColumns(dec *json.Decoder) ([]string, error) {
	var cols []lakeColumn
	if err := dec.Decode(&cols); err != nil {
		return nil, fmt.Errorf("parse Sentinel lake columns: %w", err)
	}
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.name())
	}
	return names, nil
}

// decodeLakeRows streams a Rows array straight to onRow, emitting nothing for frames that are
// not the primary result.
//
// Rows that arrive before their column schema are rejected rather than held: Kusto emits
// Columns before Rows in a DataTable frame, so the reverse order is a response shape we do not
// understand, and buffering it would mean materialising an export-sized result — the whole
// thing this frame-by-frame walk exists to avoid.
func decodeLakeRows(
	dec *json.Decoder,
	primary bool,
	emitted *bool,
	columns []string,
	onColumns func([]string) error,
	onRow func([]interface{}) error,
) (int64, error) {
	if !primary || *emitted {
		return 0, skipValue(dec)
	}
	if columns == nil {
		return 0, fmt.Errorf("Sentinel lake response sent rows before their column schema")
	}

	if err := onColumns(columns); err != nil {
		return 0, err
	}
	*emitted = true

	if err := expectDelim(dec, '['); err != nil {
		return 0, fmt.Errorf("parse Sentinel lake rows: %w", err)
	}
	var rows int64
	for dec.More() {
		var raw []json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return rows, fmt.Errorf("parse Sentinel lake row: %w", err)
		}
		if err := onRow(rawRowToExportValues(raw)); err != nil {
			return rows, err
		}
		rows++
	}
	if err := expectDelim(dec, ']'); err != nil {
		return rows, fmt.Errorf("parse Sentinel lake rows: %w", err)
	}
	return rows, nil
}

// decodeLakeObject handles the v1 object shape, {"Tables":[{"Columns":…,"Rows":…}]}. The
// opening '{' has already been consumed.
func decodeLakeObject(dec *json.Decoder, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	var rows int64
	emitted := false

	for dec.More() {
		key, err := objectKey(dec)
		if err != nil {
			return rows, fmt.Errorf("parse Sentinel lake response: %w", err)
		}
		switch key {
		case "Tables", "tables":
			if err := expectDelim(dec, '['); err != nil {
				return rows, fmt.Errorf("parse Sentinel lake tables: %w", err)
			}
			for dec.More() {
				n, failure, err := decodeLakeFrame(dec, &emitted, onColumns, onRow)
				rows += n
				if err != nil {
					return rows, err
				}
				if failure != "" {
					return rows, fmt.Errorf("Sentinel lake query failed: %s", failure)
				}
			}
			if err := expectDelim(dec, ']'); err != nil {
				return rows, fmt.Errorf("parse Sentinel lake tables: %w", err)
			}
		case "error", "Error":
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return rows, fmt.Errorf("parse Sentinel lake error: %w", err)
			}
			if detail := strings.TrimSpace(string(raw)); detail != "" && detail != "null" {
				return rows, fmt.Errorf("Sentinel lake query failed: %s", detail)
			}
		default:
			if err := skipValue(dec); err != nil {
				return rows, fmt.Errorf("parse Sentinel lake response: %w", err)
			}
		}
	}
	if err := expectDelim(dec, '}'); err != nil {
		return rows, fmt.Errorf("parse Sentinel lake response: %w", err)
	}
	return rows, nil
}
