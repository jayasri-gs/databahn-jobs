package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Log Analytics /query response shape:
//
//	{"tables":[{"name":"PrimaryResult","columns":[{"name":..,"type":..}],"rows":[[..],[..]]}]}
//
// A failed query returns a top-level "error" object. A query that exceeded the service's
// memory or ~64 MB result limit returns HTTP 200 with *both* an "error" object and a
// truncated "tables" array — see sentinelResultError.

type lawColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type lawErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Target  string `json:"target"`
}

type lawError struct {
	Code    string           `json:"code"`
	Message string           `json:"message"`
	Details []lawErrorDetail `json:"details"`
	Inner   *lawError        `json:"innererror"`
}

func (e *lawError) describe() string {
	if e == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if e.Message != "" {
		parts = append(parts, e.Message)
	} else if e.Code != "" {
		parts = append(parts, e.Code)
	}
	for _, d := range e.Details {
		if d.Message != "" {
			parts = append(parts, d.Message)
		}
	}
	if inner := e.Inner.describe(); inner != "" {
		parts = append(parts, inner)
	}
	if len(parts) == 0 {
		return "unspecified Log Analytics error"
	}
	return strings.Join(parts, ": ")
}

// sentinelResultError turns a Log Analytics error object into a failure the user can act on.
//
// This is the single most important behaviour in the Sentinel path. Log Analytics answers
// HTTP 200 with *partial* results when a query exceeds its memory or ~64 MB response limit;
// accepting that silently would publish a truncated export that looks successful. The
// wording mirrors LogAnalyticsKqlSupport.requireSuccessfulResult in backend-service.
func sentinelResultError(apiErr *lawError, rows int64) error {
	if apiErr == nil {
		return nil
	}
	detail := apiErr.describe()
	if rows > 0 {
		return fmt.Errorf(
			"Log Analytics returned partial results after %d rows (often the memory or 64 MB result limit). "+
				"Narrow the time range or add filters to the query: %s", rows, detail)
	}
	return fmt.Errorf("Log Analytics query failed: %s", detail)
}

// decodeLogAnalyticsResponse streams a /query response, invoking onColumns once before the
// first row. Rows are decoded and emitted one at a time rather than unmarshalled as a whole:
// a 64 MB response inflated into []interface{} costs several hundred MB of heap, while this
// holds one row plus the caller's encode buffer.
//
// Only the first table is exported. Log Analytics returns PrimaryResult first, and any
// further tables are query statistics, not data.
func decodeLogAnalyticsResponse(r io.Reader, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()

	if err := expectDelim(dec, '{'); err != nil {
		return 0, fmt.Errorf("parse Log Analytics response: %w", err)
	}

	var apiErr *lawError
	var rows int64
	tableIndex := 0

	for dec.More() {
		key, err := objectKey(dec)
		if err != nil {
			return rows, fmt.Errorf("parse Log Analytics response: %w", err)
		}
		switch key {
		case "tables":
			n, err := decodeLAWTables(dec, &tableIndex, onColumns, onRow)
			rows += n
			if err != nil {
				return rows, err
			}
		case "error":
			if err := dec.Decode(&apiErr); err != nil {
				return rows, fmt.Errorf("parse Log Analytics error object: %w", err)
			}
		default:
			if err := skipValue(dec); err != nil {
				return rows, fmt.Errorf("parse Log Analytics response: %w", err)
			}
		}
	}

	return rows, sentinelResultError(apiErr, rows)
}

func decodeLAWTables(dec *json.Decoder, tableIndex *int, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	if err := expectDelim(dec, '['); err != nil {
		return 0, fmt.Errorf("parse Log Analytics tables: %w", err)
	}
	var rows int64
	for dec.More() {
		if *tableIndex > 0 {
			if err := skipValue(dec); err != nil {
				return rows, fmt.Errorf("parse Log Analytics tables: %w", err)
			}
			*tableIndex++
			continue
		}
		n, err := decodeLAWTable(dec, onColumns, onRow)
		rows += n
		*tableIndex++
		if err != nil {
			return rows, err
		}
	}
	if err := expectDelim(dec, ']'); err != nil {
		return rows, fmt.Errorf("parse Log Analytics tables: %w", err)
	}
	return rows, nil
}

func decodeLAWTable(dec *json.Decoder, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	if err := expectDelim(dec, '{'); err != nil {
		return 0, fmt.Errorf("parse Log Analytics table: %w", err)
	}

	var columns []string
	// Log Analytics emits "columns" before "rows"; pending covers the reverse order so a
	// shape change degrades into extra buffering rather than a corrupt export.
	var pending [][]json.RawMessage
	var rows int64

	emit := func(raw []json.RawMessage) error {
		rows++
		return onRow(rawRowToExportValues(raw))
	}

	for dec.More() {
		key, err := objectKey(dec)
		if err != nil {
			return rows, fmt.Errorf("parse Log Analytics table: %w", err)
		}
		switch key {
		case "columns":
			var cols []lawColumn
			if err := dec.Decode(&cols); err != nil {
				return rows, fmt.Errorf("parse Log Analytics columns: %w", err)
			}
			columns = make([]string, 0, len(cols))
			for _, c := range cols {
				columns = append(columns, c.Name)
			}
			if onColumns != nil {
				if err := onColumns(columns); err != nil {
					return rows, err
				}
			}
			for _, raw := range pending {
				if err := emit(raw); err != nil {
					return rows, err
				}
			}
			pending = nil
		case "rows":
			if err := expectDelim(dec, '['); err != nil {
				return rows, fmt.Errorf("parse Log Analytics rows: %w", err)
			}
			for dec.More() {
				var raw []json.RawMessage
				if err := dec.Decode(&raw); err != nil {
					return rows, fmt.Errorf("parse Log Analytics row: %w", err)
				}
				if columns == nil {
					pending = append(pending, raw)
					continue
				}
				if err := emit(raw); err != nil {
					return rows, err
				}
			}
			if err := expectDelim(dec, ']'); err != nil {
				return rows, fmt.Errorf("parse Log Analytics rows: %w", err)
			}
		default:
			if err := skipValue(dec); err != nil {
				return rows, fmt.Errorf("parse Log Analytics table: %w", err)
			}
		}
	}

	if err := expectDelim(dec, '}'); err != nil {
		return rows, fmt.Errorf("parse Log Analytics table: %w", err)
	}
	if len(pending) > 0 {
		return rows, fmt.Errorf("Log Analytics response contained rows without a column schema")
	}
	return rows, nil
}

func rawRowToExportValues(raw []json.RawMessage) []interface{} {
	row := make([]interface{}, len(raw))
	for i, cell := range raw {
		row[i] = rawToExportValue(cell)
	}
	return row
}

// rawToExportValue converts one Log Analytics cell into a value the format encoders handle.
//
// Scalars become native Go values so CSV does not quote them twice. `dynamic` columns stay
// as json.RawMessage, which is exactly right for both encoders: format.FormatValue renders
// []byte as its literal JSON text for CSV, and json.Marshal inlines RawMessage as a nested
// value for NDJSON. Numbers stay json.Number so long integers keep their precision.
func rawToExportValue(raw json.RawMessage) interface{} {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	switch trimmed[0] {
	case '"':
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return string(trimmed)
		}
		return s
	case '{', '[':
		// Already validated by the decoder, and both encoders accept it as-is: CSV renders the
		// literal text and NDJSON inlines it. Re-compacting would copy every dynamic cell.
		return json.RawMessage(trimmed)
	case 't':
		return true
	case 'f':
		return false
	default:
		return json.Number(trimmed)
	}
}

// ParseLogAnalyticsError extracts a readable message from a non-2xx response body.
func ParseLogAnalyticsError(body []byte) string {
	var envelope struct {
		Error *lawError `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error != nil {
		return envelope.Error.describe()
	}
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		return "empty response"
	}
	if len(msg) > 512 {
		msg = msg[:512] + "…"
	}
	return msg
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	got, ok := tok.(json.Delim)
	if !ok || got != want {
		return fmt.Errorf("expected %q, got %v", want, tok)
	}
	return nil
}

func objectKey(dec *json.Decoder) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	key, ok := tok.(string)
	if !ok {
		return "", fmt.Errorf("expected object key, got %v", tok)
	}
	return key, nil
}

// skipValue discards the next value without materialising it. Decoding into a RawMessage
// would allocate the whole of an ignored table or metadata block, which defeats the point of
// streaming the response in the first place.
func skipValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	open, ok := tok.(json.Delim)
	if !ok {
		return nil // a scalar; the token above consumed it
	}
	if open != '{' && open != '[' {
		return fmt.Errorf("unexpected delimiter %q while skipping a value", open)
	}
	for depth := 1; depth > 0; {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := tok.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}
