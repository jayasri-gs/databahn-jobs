package query

import (
	"encoding/json"
	"strings"
	"testing"
)

func collectLake(t *testing.T, body string) ([]string, [][]interface{}, int64, error) {
	t.Helper()
	var columns []string
	var rows [][]interface{}
	n, err := decodeSentinelLakeResponse(strings.NewReader(body),
		func(cols []string) error { columns = cols; return nil },
		func(row []interface{}) error { rows = append(rows, row); return nil })
	return columns, rows, n, err
}

// The shape the lake API actually returns: an array of frames, of which only PrimaryResult
// carries export data.
const lakeV2Response = `[
 {"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0"},
 {"FrameType":"DataTable","TableId":0,"TableKind":"QueryProperties","TableName":"@ExtendedProperties",
  "Columns":[{"ColumnName":"Value","ColumnType":"string"}],"Rows":[["ignored"]]},
 {"FrameType":"DataTable","TableId":1,"TableKind":"PrimaryResult","TableName":"PrimaryResult",
  "Columns":[{"ColumnName":"TimeGenerated","ColumnType":"datetime"},{"ColumnName":"Account","ColumnType":"string"},{"ColumnName":"Props","ColumnType":"dynamic"}],
  "Rows":[["2026-08-20T10:00:00Z","alice",{"ip":"10.0.0.1"}],["2026-08-20T11:00:00Z","bob",null]]},
 {"FrameType":"DataTable","TableId":2,"TableKind":"QueryCompletionInformation","TableName":"QueryCompletionInformation",
  "Columns":[{"ColumnName":"EventType","ColumnType":"string"}],"Rows":[["QueryInfo"]]},
 {"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}]`

func TestDecodeSentinelLakeResponse(t *testing.T) {
	columns, rows, n, err := collectLake(t, lakeV2Response)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 2 || len(rows) != 2 {
		t.Fatalf("rows = %d, decoded %d — only PrimaryResult should be exported", n, len(rows))
	}
	if want := []string{"TimeGenerated", "Account", "Props"}; !sameColumns(columns, want) {
		t.Fatalf(gotColumns, columns)
	}
	if rows[0][1] != "alice" {
		t.Fatalf("row = %v", rows[0])
	}
	// dynamic cells stay raw so CSV renders their JSON and NDJSON nests them.
	if got, ok := rows[0][2].(json.RawMessage); !ok || string(got) != `{"ip":"10.0.0.1"}` {
		t.Fatalf("dynamic = %#v", rows[0][2])
	}
	if rows[1][2] != nil {
		t.Fatalf("null = %#v", rows[1][2])
	}
}

// A lake query can fail with HTTP 200 and report it in the completion frame — the same trap
// partial results set on the analytics tier.
func TestDecodeSentinelLakeResponseCompletionErrors(t *testing.T) {
	body := `[
 {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"A","ColumnType":"string"}],"Rows":[["one"]]},
 {"FrameType":"DataSetCompletion","HasErrors":true,"OneApiErrors":[{"error":{"message":"query exceeded limits"}}]}]`
	_, _, n, err := collectLake(t, body)
	if err == nil {
		t.Fatal("a completion frame reporting errors must fail the export")
	}
	if n != 1 {
		t.Fatalf("rows counted before the failure = %d", n)
	}
	if !strings.Contains(err.Error(), "query exceeded limits") {
		t.Fatalf("error = %v, want the service detail", err)
	}
}

func TestDecodeSentinelLakeResponseEmptyResult(t *testing.T) {
	body := `[{"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"A","ColumnType":"string"}],"Rows":[]},
 {"FrameType":"DataSetCompletion","HasErrors":false}]`
	columns, _, n, err := collectLake(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 0 {
		t.Fatalf("rows = %d", n)
	}
	// Columns still arrive, so an empty export writes a header rather than failing.
	if !sameColumns(columns, []string{"A"}) {
		t.Fatalf(gotColumns, columns)
	}
}

// Some deployments answer with the v1 object shape; backend-service accepts both, so this
// decoder does too.
func TestDecodeSentinelLakeResponseV1ObjectShape(t *testing.T) {
	body := `{"Tables":[{"TableName":"PrimaryResult",
 "Columns":[{"ColumnName":"A","ColumnType":"string"}],"Rows":[["one"],["two"]]}]}`
	columns, rows, n, err := collectLake(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 2 || len(rows) != 2 || rows[1][0] != "two" {
		t.Fatalf("rows = %v (n=%d)", rows, n)
	}
	if !sameColumns(columns, []string{"A"}) {
		t.Fatalf(gotColumns, columns)
	}
}

// Defensive: Kusto emits TableKind before Rows, but a reordered frame must not be skipped.
func TestDecodeSentinelLakeResponseRowsBeforeTableKind(t *testing.T) {
	body := `[{"FrameType":"DataTable","Rows":[["one"]],"Columns":[{"ColumnName":"A","ColumnType":"string"}],"TableKind":"PrimaryResult"}]`
	columns, rows, n, err := collectLake(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 1 || len(rows) != 1 || rows[0][0] != "one" {
		t.Fatalf("rows = %v (n=%d)", rows, n)
	}
	if !sameColumns(columns, []string{"A"}) {
		t.Fatalf(gotColumns, columns)
	}
}

func TestDecodeSentinelLakeResponseRejectsGarbage(t *testing.T) {
	for _, body := range []string{``, `"a string"`, `12`} {
		if _, _, _, err := collectLake(t, body); err == nil {
			t.Fatalf("body %q was accepted", body)
		}
	}
}

// The lake API omits Retry-After on some 429s and writes the time into the body instead.
func TestLakeRetryDelayFallsBackToMessageBody(t *testing.T) {
	if got := lakeRetryDelay("30", nil); got != 30*1e9 {
		t.Fatalf("header form = %v", got)
	}
	body := []byte(`{"error":{"message":"Request is throttled, retry after 01/01/2030 00:00:00 UTC"}}`)
	if got := lakeRetryDelay("", body); got <= 0 {
		t.Fatalf("message form = %v, want a positive delay", got)
	}
	if got := lakeRetryDelay("", []byte("no hint here")); got != 0 {
		t.Fatalf("no hint = %v, want 0 so the caller uses its ladder", got)
	}
}
