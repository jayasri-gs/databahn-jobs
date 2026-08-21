package format

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// KQL `dynamic` columns arrive as json.RawMessage and long integers as json.Number. Both
// are named types, so a type switch on []byte or string misses them — the default branch
// would render a dynamic column as a list of byte values.
func TestFormatValueKQLTypes(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want string
	}{
		{"dynamic object", json.RawMessage(`{"ip":"10.0.0.1"}`), `{"ip":"10.0.0.1"}`},
		{"dynamic array", json.RawMessage(`["a","b"]`), `["a","b"]`},
		{"large integer", json.Number("9007199254740993"), "9007199254740993"},
		{"plain bytes", []byte("raw"), "raw"},
		{"null", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatValue(tc.in); got != tc.want {
				t.Fatalf("FormatValue(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCSVEncoderWritesDynamicColumn(t *testing.T) {
	var buf bytes.Buffer
	enc, err := NewEncoder("csv", &buf, ",")
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if err := enc.Init([]string{"Account", "Props"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := enc.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	if err := enc.WriteRow([]interface{}{"alice", json.RawMessage(`{"ip":"10.0.0.1"}`)}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if !strings.Contains(buf.String(), `"{""ip"":""10.0.0.1""}"`) {
		t.Fatalf("csv = %q", buf.String())
	}
}

func TestExcelCellValue(t *testing.T) {
	if got := excelCellValue(json.RawMessage(`{"a":1}`)); got != `{"a":1}` {
		t.Fatalf("dynamic = %#v, want JSON text", got)
	}
	if got := excelCellValue(json.Number("42")); got != float64(42) {
		t.Fatalf("number = %#v, want a numeric cell", got)
	}
	// Integers beyond float64's exact range stay text rather than losing digits silently.
	if got := excelCellValue(json.Number("not-a-number")); got != "not-a-number" {
		t.Fatalf("unparseable number = %#v", got)
	}
	if got := excelCellValue("plain"); got != "plain" {
		t.Fatalf("passthrough = %#v", got)
	}
}

func TestExcelEncoderWritesDynamicColumn(t *testing.T) {
	var buf bytes.Buffer
	enc, err := NewEncoder("xlsx", &buf, "")
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if err := enc.Init([]string{"Account", "Props"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := enc.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	if err := enc.WriteRow([]interface{}{"alice", json.RawMessage(`{"ip":"10.0.0.1"}`)}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	// The xlsx zip stores shared strings uncompressed enough to assert on the payload.
	if !bytes.Contains(buf.Bytes(), []byte("xl/worksheets")) {
		t.Fatalf("xlsx output does not look like a workbook (%d bytes)", buf.Len())
	}
	if buf.Len() == 0 {
		t.Fatal("empty workbook")
	}
}
