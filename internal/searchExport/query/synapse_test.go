package query

import (
	"strings"
	"testing"
)

func TestBuildCetasSQL_Parquet(t *testing.T) {
	sql := buildCetasSQL(cetasParams{
		ExternalTable: "DatabahnExport_abc",
		DataSource:    "DatabahnDataSource_t1",
		FileFormat:    synapseParquetFileFormat,
		Location:      "unload_abc_1710000000/",
		Query:         "SELECT col1 FROM dbo.v1",
	})
	if !strings.Contains(sql, "CREATE EXTERNAL TABLE [dbo].[DatabahnExport_abc]") {
		t.Fatalf("missing external table: %s", sql)
	}
	if !strings.Contains(sql, "DATA_SOURCE = [DatabahnDataSource_t1]") {
		t.Fatalf("missing data source: %s", sql)
	}
}

func TestMapCetasFileFormat(t *testing.T) {
	if got := mapCetasFileFormat(UnloadOptions{Format: "textfile"}); got != synapseCsvFileFormat {
		t.Fatalf("csv: got %s", got)
	}
	if got := mapCetasFileFormat(UnloadOptions{Format: "json"}); got != synapseParquetFileFormat {
		t.Fatalf("json: got %s", got)
	}
}

func TestReportIDFromUnloadPath(t *testing.T) {
	got := reportIDFromUnloadPath(".databahn_out/unload_abc123_1710000000/")
	if got != "abc123" {
		t.Fatalf("got %q", got)
	}
}
