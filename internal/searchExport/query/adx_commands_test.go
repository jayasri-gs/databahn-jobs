package query

import (
	"strings"
	"testing"
)

func TestADXNamePrefixIsStableAndShort(t *testing.T) {
	reportID := "3f2b9c1e-7a44-4f0b-9d21-8c5e6a1b2d3f"
	got := ADXNamePrefix(reportID)
	if got != "databahn_export_3f2b9c1e7a44" {
		t.Fatalf("ADXNamePrefix = %q", got)
	}
	if ADXNamePrefix(reportID) != got {
		t.Fatal("ADXNamePrefix must be stable across calls so retries reuse staged blobs")
	}
	if ADXNamePrefix("") != "databahn_export_report" {
		t.Fatalf("empty report id fallback = %q", ADXNamePrefix(""))
	}
}

func TestADXExportFormat(t *testing.T) {
	tests := map[string]string{
		"csv":      "csv",
		"tsv":      "tsv",
		"json":     "json",
		"parquet":  "parquet",
		"textfile": "parquet",
		"":         "parquet",
	}
	for in, want := range tests {
		if got := ADXExportFormat(UnloadOptions{Format: in}); got != want {
			t.Fatalf("ADXExportFormat(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildADXExportCommand_CSV(t *testing.T) {
	cmd := BuildADXExportCommand(
		"Logs\n| where Timestamp > ago(1d)\n| take 1000000",
		"https://acct.blob.core.windows.net/container?sv=2024-11-04&sig=abc",
		"csv",
		"databahn_export_abc123",
	)

	for _, want := range []string{
		".export async to csv",
		`(h@"https://acct.blob.core.windows.net/container?sv=2024-11-04&sig=abc")`,
		`namePrefix="databahn_export_abc123"`,
		"includeHeaders=none",
		"encoding=UTF8NoBOM",
		"sizeLimit=1073741824",
		"<| set notruncation;",
		"| take 1000000",
	} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("export command missing %q:\n%s", want, cmd)
		}
	}
}

func TestBuildADXExportCommand_ParquetOmitsCSVProperties(t *testing.T) {
	cmd := BuildADXExportCommand("Logs", "https://acct.blob.core.windows.net/c?sig=x", "parquet", "p")
	if strings.Contains(cmd, "includeHeaders") || strings.Contains(cmd, "encoding=") {
		t.Fatalf("parquet export must not carry csv-only properties:\n%s", cmd)
	}
	if !strings.Contains(cmd, ".export async to parquet") {
		t.Fatalf("unexpected command:\n%s", cmd)
	}
}

func TestBuildADXOperationCommands(t *testing.T) {
	const opID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	if got := BuildADXShowOperationCommand(opID); got != `.show operations "`+opID+`"` {
		t.Fatalf("show operations = %q", got)
	}
	if got := BuildADXShowOperationDetailsCommand(opID); got != `.show operation "`+opID+`" details` {
		t.Fatalf("show operation details = %q", got)
	}
	if got := BuildADXCancelOperationCommand(opID); got != `.cancel operation "`+opID+`"` {
		t.Fatalf("cancel operation = %q", got)
	}
}

func TestBuildADXSchemaQuery(t *testing.T) {
	got := BuildADXSchemaQuery("Logs | take 10")
	if !strings.HasPrefix(got, "Logs | take 10") || !strings.Contains(got, "| getschema") ||
		!strings.Contains(got, "sort by ColumnOrdinal asc") || !strings.Contains(got, "project ColumnName") {
		t.Fatalf("schema query = %q", got)
	}
}

func TestMapADXOperationState(t *testing.T) {
	tests := map[string]string{
		"Completed":  QueryStateSucceeded,
		"InProgress": QueryStateRunning,
		"Scheduled":  QueryStateQueued,
		"Throttled":  QueryStateQueued,
		"Failed":     QueryStateFailed,
		"BadInput":   QueryStateFailed,
		"Canceled":   QueryStateCancelled,
		"Abandoned":  QueryStateCancelled,
		"nonsense":   QueryStateFailed,
	}
	for in, want := range tests {
		if got := MapADXOperationState(in); got != want {
			t.Fatalf("MapADXOperationState(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQuoteKustoStringEscapes(t *testing.T) {
	if got := quoteKustoString(`a"b\c`); got != `"a\"b\\c"` {
		t.Fatalf("quoteKustoString = %s", got)
	}
}

func TestParseADXOperationID(t *testing.T) {
	body := []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"OperationId","DataType":"Guid","ColumnType":"guid"}],"Rows":[["11111111-2222-3333-4444-555555555555"]]}]}`)
	got, err := ParseADXOperationID(body)
	if err != nil {
		t.Fatalf("ParseADXOperationID: %v", err)
	}
	if got != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("operation id = %q", got)
	}
}

func TestParseADXOperationID_UnnamedColumnFallback(t *testing.T) {
	body := []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"Column1"}],"Rows":[["op-id"]]}]}`)
	got, err := ParseADXOperationID(body)
	if err != nil {
		t.Fatalf("ParseADXOperationID: %v", err)
	}
	if got != "op-id" {
		t.Fatalf("operation id = %q", got)
	}
}

func TestParseADXOperationID_NoRows(t *testing.T) {
	body := []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"OperationId"}],"Rows":[]}]}`)
	if _, err := ParseADXOperationID(body); err == nil {
		t.Fatal("expected error when the export command returns no rows")
	}
}

func TestParseADXOperationStatus(t *testing.T) {
	body := []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"OperationId"},{"ColumnName":"State"},{"ColumnName":"Status"}],"Rows":[["op","Failed","Query execution has exceeded memory limit"]]}]}`)
	state, status, err := ParseADXOperationStatus(body)
	if err != nil {
		t.Fatalf(errParseADXOpStatus, err)
	}
	if state != "Failed" || status != "Query execution has exceeded memory limit" {
		t.Fatalf("state=%q status=%q", state, status)
	}
	if MapADXOperationState(state) != QueryStateFailed {
		t.Fatal("Failed state must map to FAILED")
	}
}

func TestParseADXExportedPaths(t *testing.T) {
	body := []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"Path"},{"ColumnName":"NumRecords"}],"Rows":[["https://acct.blob.core.windows.net/c/databahn_export_ab_1_x.csv",12],["https://acct.blob.core.windows.net/c/databahn_export_ab_2_y.csv",8],[null,0]]}]}`)
	paths, err := ParseADXExportedPaths(body)
	if err != nil {
		t.Fatalf("ParseADXExportedPaths: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("paths = %v", paths)
	}
	if !strings.HasSuffix(paths[0], "_1_x.csv") {
		t.Fatalf("unexpected first path %q", paths[0])
	}
}

func TestParseADXSchemaColumns(t *testing.T) {
	body := []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"ColumnName"}],"Rows":[["Timestamp"],["Message"],["Level"]]}]}`)
	cols, err := ParseADXSchemaColumns(body)
	if err != nil {
		t.Fatalf("ParseADXSchemaColumns: %v", err)
	}
	if len(cols) != 3 || cols[0] != "Timestamp" || cols[2] != "Level" {
		t.Fatalf(gotColumns, cols)
	}
}

func TestParseKustoV1Response_Errors(t *testing.T) {
	if _, err := parseKustoV1Response([]byte("not json")); err == nil {
		t.Fatal("expected parse error")
	}
	if _, err := parseKustoV1Response([]byte(`{"Tables":[]}`)); err == nil {
		t.Fatal("expected error for empty table list")
	}
}

func TestParseADXError(t *testing.T) {
	body := []byte(`{"error":{"code":"BadRequest","message":"Request is invalid","@innererror":{"message":"Syntax error at position 5"}}}`)
	if got := ParseADXError(body); got != "Syntax error at position 5" {
		t.Fatalf("ParseADXError = %q", got)
	}
	if got := ParseADXError([]byte("plain failure")); got != "plain failure" {
		t.Fatalf("ParseADXError fallback = %q", got)
	}
}

func TestParseADXOperationStatus_MultipleRows(t *testing.T) {
	// A retried export reports the same operation id on two nodes; the unfinished
	// attempt must win so the pipeline keeps waiting instead of declaring failure.
	body := []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"State"},{"ColumnName":"Status"}],"Rows":[["Failed","node lost"],["InProgress",""]]}]}`)
	state, _, err := ParseADXOperationStatus(body)
	if err != nil {
		t.Fatalf(errParseADXOpStatus, err)
	}
	if MapADXOperationState(state) != QueryStateRunning {
		t.Fatalf("state = %q, want a running state", state)
	}

	// All terminal: the most recent row wins.
	body = []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"State"},{"ColumnName":"Status"}],"Rows":[["Failed","first"],["Completed",""]]}]}`)
	state, _, err = ParseADXOperationStatus(body)
	if err != nil {
		t.Fatalf(errParseADXOpStatus, err)
	}
	if MapADXOperationState(state) != QueryStateSucceeded {
		t.Fatalf("state = %q, want SUCCEEDED", state)
	}
}

func TestParseADXOperationStatus_MissingStateColumn(t *testing.T) {
	body := []byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"OperationId"}],"Rows":[["op"]]}]}`)
	if _, _, err := ParseADXOperationStatus(body); err == nil {
		t.Fatal("expected error when State column is absent")
	}
}
