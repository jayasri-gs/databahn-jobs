package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"go.uber.org/zap"
)

// fakeStagingReader records what the executor listed and deleted.
type fakeStagingReader struct {
	existing     []string
	listedPrefix string
	deleted      []string
	deleteErr    error
}

func (f *fakeStagingReader) ListFiles(_ context.Context, prefix string) ([]string, error) {
	f.listedPrefix = prefix
	return f.existing, nil
}
func (f *fakeStagingReader) ParseManifest(context.Context, string) ([]string, error) { return nil, nil }
func (f *fakeStagingReader) StreamToUploader(context.Context, upload.CloudUploader, []string, []byte, string, string, *zap.Logger) (int64, int64, error) {
	return 0, 0, nil
}
func (f *fakeStagingReader) StreamRows(context.Context, []string, func([]interface{}) error) error {
	return nil
}
func (f *fakeStagingReader) DeleteFiles(_ context.Context, files []string) error {
	f.deleted = append(f.deleted, files...)
	return f.deleteErr
}
func (f *fakeStagingReader) Columns() []string { return nil }

func newExportExecutor(t *testing.T, reader *fakeStagingReader, handler http.HandlerFunc) *ADXExecutor {
	t.Helper()
	exec, _ := newTestADXExecutor(t, handler)
	exec.cfg.NamePrefix = "databahn_export_abc123def456"
	exec.cfg.StagingBlob = &destination.AzureBlobConfig{
		AuthType:         "AUTH_CONNECTION_STRING",
		Container:        "exports",
		ConnectionString: "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net",
	}
	exec.newStagingReader = func(string) (unload.StagingReader, error) { return reader, nil }
	return exec
}

// namePrefix is stable across retries so a resumed job finds its blobs. That means a failed
// attempt's partial output is still under the prefix when a fresh export starts, and the
// upload phase lists the whole prefix — so it must be cleared first, or stale blobs get
// concatenated with the new export's output.
func TestExecuteUnloadAsyncClearsStalePrefix(t *testing.T) {
	stale := []string{
		"databahn_export_abc123def456_1_aaa.csv",
		"databahn_export_abc123def456_2_bbb.csv",
	}
	reader := &fakeStagingReader{existing: stale}

	var exportIssued bool
	exec := newExportExecutor(t, reader, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]string
		_ = json.Unmarshal(body, &payload)
		if len(payload["csl"]) > 0 && payload["csl"][0] == '.' {
			exportIssued = true
		}
		_, _ = w.Write([]byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"OperationId"}],"Rows":[["op-42"]]}]}`))
	})

	id, err := exec.ExecuteUnloadAsync(context.Background(), "Logs | take 1", "SecurityLogs", "", UnloadOptions{Format: "csv"})
	if err != nil {
		t.Fatalf("ExecuteUnloadAsync: %v", err)
	}
	if id != "op-42" {
		t.Fatalf("operation id = %q", id)
	}
	if !exportIssued {
		t.Fatal("no control command was issued")
	}
	if reader.listedPrefix != exec.cfg.NamePrefix {
		t.Fatalf("listed prefix = %q, want %q", reader.listedPrefix, exec.cfg.NamePrefix)
	}
	if len(reader.deleted) != len(stale) {
		t.Fatalf("deleted %v, want the stale blobs %v", reader.deleted, stale)
	}
}

// A clean prefix must not trigger a delete call.
func TestExecuteUnloadAsyncSkipsDeleteWhenPrefixIsEmpty(t *testing.T) {
	reader := &fakeStagingReader{}
	exec := newExportExecutor(t, reader, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"OperationId"}],"Rows":[["op-1"]]}]}`))
	})

	if _, err := exec.ExecuteUnloadAsync(context.Background(), "Logs", "SecurityLogs", "", UnloadOptions{Format: "csv"}); err != nil {
		t.Fatalf("ExecuteUnloadAsync: %v", err)
	}
	if len(reader.deleted) != 0 {
		t.Fatalf("deleted %v on a clean prefix", reader.deleted)
	}
}

// If the stale blobs cannot be removed, the export must not start — running it anyway is what
// produces the corrupt concatenation this guard exists to prevent.
func TestExecuteUnloadAsyncFailsWhenPrefixCannotBeCleared(t *testing.T) {
	reader := &fakeStagingReader{
		existing:  []string{"databahn_export_abc123def456_1_aaa.csv"},
		deleteErr: fmt.Errorf("blob delete refused"),
	}
	var exportIssued bool
	exec := newExportExecutor(t, reader, func(w http.ResponseWriter, r *http.Request) {
		exportIssued = true
		_, _ = w.Write([]byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"OperationId"}],"Rows":[["op-1"]]}]}`))
	})

	if _, err := exec.ExecuteUnloadAsync(context.Background(), "Logs", "SecurityLogs", "", UnloadOptions{Format: "csv"}); err == nil {
		t.Fatal("expected the export to fail when stale blobs remain")
	}
	if exportIssued {
		t.Fatal("export was issued despite stale blobs still being present")
	}
}
