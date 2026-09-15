package query

import (
	"errors"
	"strings"
	"testing"
)

func collectSplunk(t *testing.T, body string) ([]string, [][]interface{}, int64, SplunkStreamMetadata, error) {
	t.Helper()
	var columns []string
	var rows [][]interface{}
	var onColumnsCalls int
	n, meta, err := decodeSplunkJsonRowsResponseWithMetadata(strings.NewReader(body),
		func(cols []string) error {
			onColumnsCalls++
			columns = cols
			return nil
		},
		func(row []interface{}) error {
			rows = append(rows, row)
			return nil
		})
	if onColumnsCalls > 1 {
		t.Fatalf("onColumns called %d times, want 1", onColumnsCalls)
	}
	return columns, rows, n, meta, err
}

func TestDecodeSplunkJsonRowsSingleFrame(t *testing.T) {
	body := `{"preview":false,"fields":["TimeGenerated","Account"],"rows":[["2026-08-20T10:00:00Z","alice"],["2026-08-20T11:00:00Z","bob"]]}`
	columns, rows, n, _, err := collectSplunk(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 2 || len(rows) != 2 {
		t.Fatalf("rows = %d, decoded %d", n, len(rows))
	}
	if !sameSplunkColumns(columns, []string{"TimeGenerated", "Account"}) {
		t.Fatalf("columns = %v", columns)
	}
	if rows[1][1] != "bob" {
		t.Fatalf("row = %v", rows[1])
	}
}

func TestDecodeSplunkJsonRowsMultipleFrames(t *testing.T) {
	body := `{"preview":false,"fields":["A"],"rows":[["one"]]}` +
		`{"preview":false,"fields":["A"],"rows":[["two"]]}`
	_, rows, n, _, err := collectSplunk(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 2 || len(rows) != 2 {
		t.Fatalf("rows = %d, decoded %d", n, len(rows))
	}
	if rows[0][0] != "one" || rows[1][0] != "two" {
		t.Fatalf("rows = %v", rows)
	}
}

func TestDecodeSplunkJsonRowsSkipsPreviewFrames(t *testing.T) {
	body := `{"preview":true,"fields":["A"],"rows":[["partial"]]}` +
		`{"preview":false,"fields":["A"],"rows":[["final"]]}`
	_, rows, n, meta, err := collectSplunk(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 1 || len(rows) != 1 {
		t.Fatalf("rows = %d, decoded %d", n, len(rows))
	}
	if rows[0][0] != "final" {
		t.Fatalf("row = %v", rows[0])
	}
	if meta.PreviewFramesSkipped != 1 {
		t.Fatalf("previewFramesSkipped = %d, want 1", meta.PreviewFramesSkipped)
	}
}

func TestDecodeSplunkJsonRowsProjectsReorderedFields(t *testing.T) {
	body := `{"preview":false,"fields":["A","B","C"],"rows":[["a1","b1","c1"]]}` +
		`{"preview":false,"fields":["C","A","B"],"rows":[["c2","a2","b2"]]}`
	_, rows, n, _, err := collectSplunk(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 2 {
		t.Fatalf("rows = %d", n)
	}
	if rows[1][0] != "a2" || rows[1][1] != "b2" || rows[1][2] != "c2" {
		t.Fatalf("projected row = %v", rows[1])
	}
}

func TestDecodeSplunkJsonRowsDropsUnknownFieldsOnce(t *testing.T) {
	body := `{"preview":false,"fields":["A","B"],"rows":[["a1","b1"]]}` +
		`{"preview":false,"fields":["B","C","A"],"rows":[["b2","c2","a2"]]}` +
		`{"preview":false,"fields":["C","A","B"],"rows":[["c3","a3","b3"]]}`
	_, rows, n, meta, err := collectSplunk(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 3 {
		t.Fatalf("rows = %d", n)
	}
	if len(rows[1]) != 2 || rows[1][0] != "a2" || rows[1][1] != "b2" {
		t.Fatalf("second row = %v", rows[1])
	}
	if len(meta.DroppedFieldNames) != 1 || meta.DroppedFieldNames[0] != "C" {
		t.Fatalf("droppedFieldNames = %v, want [C]", meta.DroppedFieldNames)
	}
}

func TestDecodeSplunkJsonRowsParsesMessages(t *testing.T) {
	body := `{"messages":[{"type":"INFO","text":"Search started"}]}` +
		`{"preview":false,"fields":["A"],"rows":[["one"]]}` +
		`{"messages":[{"type":"WARN","text":"Result set truncated at 50000 rows"}]}`
	_, _, n, meta, err := collectSplunk(t, body)
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 1 {
		t.Fatalf("rows = %d", n)
	}
	if len(meta.Messages) != 2 {
		t.Fatalf("messages = %v", meta.Messages)
	}
	if meta.Messages[1].Type != "WARN" {
		t.Fatalf("message type = %q", meta.Messages[1].Type)
	}
	if len(meta.TruncationHints) != 1 || !strings.Contains(meta.TruncationHints[0], "truncated") {
		t.Fatalf("truncationHints = %v", meta.TruncationHints)
	}
}

func TestDecodeSplunkJsonRowsMalformedJSON(t *testing.T) {
	_, _, n, _, err := collectSplunk(t, `{"fields":["A"],"rows":[[`)
	if err == nil {
		t.Fatal("expected malformed JSON to fail")
	}
	if n != 0 {
		t.Fatalf("rows = %d, want 0", n)
	}
}

func TestDecodeSplunkJsonRowsOnColumnsCalledOnce(t *testing.T) {
	body := `{"preview":false,"fields":["A"],"rows":[["one"]]}` +
		`{"preview":false,"fields":["A"],"rows":[["two"]]}`
	var calls int
	var columns []string
	n, _, err := decodeSplunkJsonRowsResponseWithMetadata(strings.NewReader(body),
		func(cols []string) error {
			calls++
			columns = cols
			return nil
		},
		func(row []interface{}) error { return nil })
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if calls != 1 {
		t.Fatalf("onColumns calls = %d, want 1", calls)
	}
	if !sameSplunkColumns(columns, []string{"A"}) {
		t.Fatalf("columns = %v", columns)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
}

func TestDecodeSplunkJsonRowsOnRowErrorPropagates(t *testing.T) {
	want := errors.New("encoder failed")
	body := `{"preview":false,"fields":["A"],"rows":[["one"],["two"]]}`
	n, err := decodeSplunkJsonRowsResponse(strings.NewReader(body), nil,
		func(row []interface{}) error {
			if row[0] == "two" {
				return want
			}
			return nil
		})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if n != 1 {
		t.Fatalf("rows = %d, want 1 before callback failure", n)
	}
}

func TestDecodeSplunkJsonRowsEmptyStream(t *testing.T) {
	var onColumnsCalls int
	n, meta, err := decodeSplunkJsonRowsResponseWithMetadata(strings.NewReader(""),
		func([]string) error {
			onColumnsCalls++
			return nil
		},
		func([]interface{}) error { return nil })
	if err != nil {
		t.Fatalf(errDecodeResponse, err)
	}
	if n != 0 {
		t.Fatalf("rows = %d, want 0", n)
	}
	if onColumnsCalls != 0 {
		t.Fatalf("onColumns calls = %d, want 0", onColumnsCalls)
	}
	if len(meta.Messages) != 0 {
		t.Fatalf("metadata = %+v", meta)
	}
}

func TestDecodeSplunkJsonRowsErrorFrame(t *testing.T) {
	_, _, n, _, err := collectSplunk(t, `{"error":"The search failed badly"}`)
	if err == nil {
		t.Fatal("expected error frame to fail decode")
	}
	if n != 0 {
		t.Fatalf("rows = %d", n)
	}
	if !strings.Contains(err.Error(), "search failed badly") {
		t.Fatalf("err = %q", err)
	}
}

func TestDecodeSplunkJsonRowsOnColumnsErrorPropagates(t *testing.T) {
	want := errors.New("columns rejected")
	body := `{"preview":false,"fields":["A"],"rows":[["one"]]}`
	_, err := decodeSplunkJsonRowsResponse(strings.NewReader(body),
		func([]string) error { return want },
		func([]interface{}) error { return nil })
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}
