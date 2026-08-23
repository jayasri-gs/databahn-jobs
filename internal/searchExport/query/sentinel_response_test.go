package query

import (
	"encoding/json"
	"strings"
	"testing"
)

// collect drains a response body through the decoder, returning columns and rows.
func collect(t *testing.T, body string) ([]string, [][]interface{}, int64, error) {
	t.Helper()
	var columns []string
	var rows [][]interface{}
	n, err := decodeLogAnalyticsResponse(strings.NewReader(body),
		func(cols []string) error {
			columns = cols
			return nil
		},
		func(row []interface{}) error {
			rows = append(rows, row)
			return nil
		})
	return columns, rows, n, err
}

func TestDecodeLogAnalyticsResponse(t *testing.T) {
	body := `{"tables":[{"name":"PrimaryResult",` +
		`"columns":[{"name":"TimeGenerated","type":"datetime"},{"name":"Count","type":"long"},{"name":"Props","type":"dynamic"}],` +
		`"rows":[["2026-08-20T10:00:00Z",42,{"a":1}],["2026-08-20T11:00:00Z",null,["x","y"]]]}]}`

	columns, rows, n, err := collect(t, body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != 2 || len(rows) != 2 {
		t.Fatalf("rows = %d, decoded %d", n, len(rows))
	}
	if want := []string{"TimeGenerated", "Count", "Props"}; !sameColumns(columns, want) {
		t.Fatalf("columns = %v, want %v", columns, want)
	}
	if rows[0][0] != "2026-08-20T10:00:00Z" {
		t.Fatalf("datetime = %#v", rows[0][0])
	}
	if got, ok := rows[0][1].(json.Number); !ok || got.String() != "42" {
		t.Fatalf("long = %#v, want json.Number(42)", rows[0][1])
	}
	// dynamic columns stay raw so CSV renders their JSON text and NDJSON nests them.
	if got, ok := rows[0][2].(json.RawMessage); !ok || string(got) != `{"a":1}` {
		t.Fatalf("dynamic = %#v", rows[0][2])
	}
	if rows[1][1] != nil {
		t.Fatalf("null = %#v, want nil", rows[1][1])
	}
	if got, ok := rows[1][2].(json.RawMessage); !ok || string(got) != `["x","y"]` {
		t.Fatalf("dynamic array = %#v", rows[1][2])
	}
}

// The result-limit case: HTTP 200, a truncated table, and an error object. Accepting this
// silently would publish a short export that looks successful.
func TestDecodeLogAnalyticsResponsePartialResultsFail(t *testing.T) {
	body := `{"tables":[{"name":"PrimaryResult","columns":[{"name":"A","type":"string"}],"rows":[["one"],["two"]]}],` +
		`"error":{"code":"PartialError","message":"Query result set has exceeded the internal record count limit",` +
		`"details":[{"code":"ResultSetTooLarge","message":"maximum result size exceeded"}]}}`

	_, rows, n, err := collect(t, body)
	if err == nil {
		t.Fatal("expected partial results to fail the export")
	}
	if n != 2 || len(rows) != 2 {
		t.Fatalf("rows should still be counted before failing: n=%d rows=%d", n, len(rows))
	}
	for _, want := range []string{"partial results after 2 rows", "Narrow the time range", "maximum result size exceeded"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err, want)
		}
	}
}

func TestDecodeLogAnalyticsResponseErrorWithoutRows(t *testing.T) {
	body := `{"error":{"code":"BadArgumentError","message":"'foo' operator not supported"}}`
	_, _, n, err := collect(t, body)
	if err == nil {
		t.Fatal("expected error")
	}
	if n != 0 {
		t.Fatalf("rows = %d, want 0", n)
	}
	if !strings.Contains(err.Error(), "Log Analytics query failed") || strings.Contains(err.Error(), "partial") {
		t.Fatalf("error = %q, want a plain failure", err)
	}
}

func TestDecodeLogAnalyticsResponseEmptyResult(t *testing.T) {
	body := `{"tables":[{"name":"PrimaryResult","columns":[{"name":"A","type":"string"}],"rows":[]}]}`
	columns, rows, n, err := collect(t, body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != 0 || len(rows) != 0 {
		t.Fatalf("expected no rows, got %d", n)
	}
	// Columns still arrive, so an empty export writes a header rather than failing.
	if !sameColumns(columns, []string{"A"}) {
		t.Fatalf("columns = %v", columns)
	}
}

// Only PrimaryResult is exported; trailing statistics tables are skipped.
func TestDecodeLogAnalyticsResponseIgnoresExtraTables(t *testing.T) {
	body := `{"tables":[` +
		`{"name":"PrimaryResult","columns":[{"name":"A","type":"string"}],"rows":[["one"]]},` +
		`{"name":"QueryStatistics","columns":[{"name":"B","type":"long"}],"rows":[[1],[2]]}]}`
	columns, rows, n, err := collect(t, body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != 1 || len(rows) != 1 || rows[0][0] != "one" {
		t.Fatalf("rows = %v (n=%d)", rows, n)
	}
	if !sameColumns(columns, []string{"A"}) {
		t.Fatalf("columns = %v", columns)
	}
}

// Defensive: Log Analytics emits columns before rows, but a reordered payload must not
// silently emit rows under an unknown schema.
func TestDecodeLogAnalyticsResponseRowsBeforeColumns(t *testing.T) {
	body := `{"tables":[{"name":"PrimaryResult","rows":[["one"]],"columns":[{"name":"A","type":"string"}]}]}`
	columns, rows, n, err := collect(t, body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != 1 || len(rows) != 1 || rows[0][0] != "one" {
		t.Fatalf("rows = %v (n=%d)", rows, n)
	}
	if !sameColumns(columns, []string{"A"}) {
		t.Fatalf("columns = %v", columns)
	}
}

func TestDecodeLogAnalyticsResponseRowCallbackErrorStops(t *testing.T) {
	body := `{"tables":[{"name":"PrimaryResult","columns":[{"name":"A","type":"string"}],"rows":[["one"],["two"],["three"]]}]}`
	var seen int
	_, err := decodeLogAnalyticsResponse(strings.NewReader(body), nil, func([]interface{}) error {
		seen++
		if seen == 2 {
			return errSentinelRowBudget
		}
		return nil
	})
	if err != errSentinelRowBudget {
		t.Fatalf("err = %v, want the budget sentinel", err)
	}
	if seen != 2 {
		t.Fatalf("callback ran %d times, want 2", seen)
	}
}

func TestRawToExportValue(t *testing.T) {
	cases := []struct {
		raw  string
		want interface{}
	}{
		{`null`, nil},
		{`"hello"`, "hello"},
		{`"quote \" inside"`, `quote " inside`},
		{`true`, true},
		{`false`, false},
		// Returned as-is: the decoder has already validated it, and re-compacting would copy
		// every dynamic cell in the response.
		{`{"a":1}`, json.RawMessage(`{"a":1}`)},
	}
	for _, tc := range cases {
		got := rawToExportValue(json.RawMessage(tc.raw))
		if raw, ok := tc.want.(json.RawMessage); ok {
			gotRaw, ok := got.(json.RawMessage)
			if !ok || string(gotRaw) != string(raw) {
				t.Fatalf("rawToExportValue(%s) = %#v, want %s", tc.raw, got, raw)
			}
			continue
		}
		if got != tc.want {
			t.Fatalf("rawToExportValue(%s) = %#v, want %#v", tc.raw, got, tc.want)
		}
	}
	// Long integers keep their precision instead of round-tripping through float64.
	got := rawToExportValue(json.RawMessage(`9007199254740993`))
	if num, ok := got.(json.Number); !ok || num.String() != "9007199254740993" {
		t.Fatalf("large integer = %#v", got)
	}
}

func TestParseLogAnalyticsError(t *testing.T) {
	msg := ParseLogAnalyticsError([]byte(`{"error":{"code":"Forbidden","message":"denied","innererror":{"message":"missing Log Analytics Reader"}}}`))
	for _, want := range []string{"denied", "missing Log Analytics Reader"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q missing %q", msg, want)
		}
	}
	if got := ParseLogAnalyticsError([]byte("upstream timeout")); got != "upstream timeout" {
		t.Fatalf("plain body = %q", got)
	}
	if got := ParseLogAnalyticsError(nil); got != "empty response" {
		t.Fatalf("empty body = %q", got)
	}
}
