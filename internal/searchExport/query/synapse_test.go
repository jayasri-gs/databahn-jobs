package query

import (
	"context"
	"fmt"
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

func TestBuildDropExternalTableSQL(t *testing.T) {
	sql := buildDropExternalTableSQL("DatabahnExport_abc")
	if !strings.Contains(sql, "sys.external_tables") {
		t.Fatalf("expected sys.external_tables guard: %s", sql)
	}
	if !strings.Contains(sql, "DROP EXTERNAL TABLE [dbo].[DatabahnExport_abc]") {
		t.Fatalf("missing drop: %s", sql)
	}
	if strings.Contains(sql, "DROP EXTERNAL TABLE IF EXISTS") {
		t.Fatalf("must not use unsupported DROP IF EXISTS syntax: %s", sql)
	}
}

func TestExportProbeQuery(t *testing.T) {
	q := "SELECT * FROM events WHERE id = 1"
	got := exportProbeQuery(q)
	if !strings.Contains(got, "SELECT TOP 1 * FROM (") || !strings.Contains(got, q) {
		t.Fatalf("unexpected probe sql: %s", got)
	}
}

func TestCheckQueryStatus_PropagatesCetasConnectionError(t *testing.T) {
	exec := &SynapseExecutor{}
	exec.recordCetasResult(fmt.Errorf("Parquet magic bytes not found"))

	status, err := exec.CheckQueryStatus(context.Background(), "132")
	if status != QueryStateFailed {
		t.Fatalf("status: got %q", status)
	}
	if err == nil || !strings.Contains(err.Error(), "Parquet magic bytes not found") {
		t.Fatalf("err: %v", err)
	}
	if !isDefinitiveCetasFailure(err) {
		t.Fatalf("expected definitive CETAS failure")
	}
}

func TestCheckQueryStatus_FailsWhenCetasCompletesWithoutStaging(t *testing.T) {
	exec := &SynapseExecutor{stagingPrefix: "unload_abc/"}
	exec.recordCetasResult(nil)

	status, err := exec.CheckQueryStatus(context.Background(), "132")
	if status != QueryStateFailed {
		t.Fatalf("status: got %q", status)
	}
	if err == nil || !strings.Contains(err.Error(), "no staging output") {
		t.Fatalf("err: %v", err)
	}
}
