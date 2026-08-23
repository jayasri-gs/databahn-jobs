package pipeline

import (
	"context"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"go.uber.org/zap"
)

type mockADX struct {
	mockAthena
	stagingReader *mockStagingReader
	cancelledID   string
	connected     bool
}

func (m *mockADX) Engine() string            { return query.EngineADX }
func (m *mockADX) GetOutputLocation() string { return "databahn_export_abc123" }

func (m *mockADX) Connect(context.Context) error {
	m.connected = true
	return nil
}

func (m *mockADX) CancelQueryExecution(_ context.Context, executionID string) error {
	m.cancelledID = executionID
	return nil
}

func (m *mockADX) NewStagingReader(string) (unload.StagingReader, error) {
	if m.stagingReader == nil {
		m.stagingReader = &mockStagingReader{}
	}
	return m.stagingReader, nil
}

func newADXPipeline(format, delimiter string) *Pipeline {
	return New(PipelineConfig{TempDir: "/tmp"}, testReportID, "Search Export - 2026-07-15 10:00:00",
		&models.SearchExportConfig{Query: "Logs | take 10", Format: format, Delimiter: delimiter}, Deps{Unload: &mockADX{}, Uploader: &mockUploader{}}, zap.NewNop())
}

func TestADXUnloadOptions(t *testing.T) {
	tests := []struct {
		format    string
		delimiter string
		want      string
		direct    bool
	}{
		{"csv", ",", "csv", true},
		{"csv", "", "csv", true},
		{"csv", "\t", "tsv", true},
		{"csv", "|", "parquet", false}, // Kusto csv is comma-only; encoder handles the rest
		{"csv", "||", "parquet", false},
		{"json", "", "json", true},
		{"xlsx", "", "parquet", false},
		{"avro", "", "parquet", false},
	}
	for _, tc := range tests {
		p := newADXPipeline(tc.format, tc.delimiter)
		opts := p.unloadOptions()
		if opts.Format != tc.want {
			t.Fatalf("unloadOptions(%q,%q).Format = %q, want %q", tc.format, tc.delimiter, opts.Format, tc.want)
		}
		if got := p.useDirectUpload(opts); got != tc.direct {
			t.Fatalf("useDirectUpload(%q,%q) = %v, want %v", tc.format, tc.delimiter, got, tc.direct)
		}
	}
}

func TestAthenaUnloadOptionsUnchangedByADXBranch(t *testing.T) {
	p := New(PipelineConfig{TempDir: "/tmp"}, testReportID, "Search Export - 2026-07-15 10:00:00",
		&models.SearchExportConfig{Query: "SELECT 1", Format: "csv", Delimiter: ","}, Deps{Unload: &mockAthena{}, Uploader: &mockUploader{}}, zap.NewNop())
	opts := p.unloadOptions()
	if opts.Format != "textfile" {
		t.Fatalf("athena csv unload format = %q, want textfile", opts.Format)
	}
	if !p.useDirectUpload(opts) {
		t.Fatal("athena textfile unload should stream directly")
	}
}

func TestCleanupADXStaging_CancelsOperationAndDeletesStagedBlobs(t *testing.T) {
	reportID := "3f2b9c1e-7a44-4f0b-9d21-8c5e6a1b2d3f"
	staged := []string{
		query.ADXNamePrefix(reportID) + "_1_aaa.csv",
		query.ADXNamePrefix(reportID) + "_2_bbb.csv",
	}
	exec := &mockADX{stagingReader: &mockStagingReader{files: staged}}

	CleanupADXStaging(context.Background(), exec, reportID, "op-123", "/tmp", zap.NewNop())

	if exec.cancelledID != "op-123" {
		t.Fatalf("cancelled operation = %q, want op-123", exec.cancelledID)
	}
	if len(exec.stagingReader.deleted) != 1 || len(exec.stagingReader.deleted[0]) != 2 {
		t.Fatalf("staged blobs not deleted: %v", exec.stagingReader.deleted)
	}
}

func TestCleanupADXStaging_WithoutOperationIDSkipsCancel(t *testing.T) {
	exec := &mockADX{stagingReader: &mockStagingReader{}}

	CleanupADXStaging(context.Background(), exec, testReportID, "", "/tmp", zap.NewNop())

	if exec.connected {
		t.Error("no operation id means nothing to cancel, so no connection is needed")
	}
	if exec.cancelledID != "" {
		t.Errorf("unexpected cancel of %q", exec.cancelledID)
	}
	if len(exec.stagingReader.deleted) != 0 {
		t.Errorf("nothing staged, so nothing should be deleted: %v", exec.stagingReader.deleted)
	}
}

func TestCleanupADXStaging_NilExecutorIsNoop(t *testing.T) {
	CleanupADXStaging(context.Background(), nil, testReportID, "op", "/tmp", zap.NewNop())
}
