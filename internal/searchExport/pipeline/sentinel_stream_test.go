package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
)

// capturingUploader keeps the uploaded bytes so a test can assert the exported file.
type capturingUploader struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	container string
	blobName  string
	completed bool
	aborted   bool
}

func (u *capturingUploader) Init(_ context.Context, container, blobName, _ string) error {
	u.container, u.blobName = container, blobName
	return nil
}

func (u *capturingUploader) UploadPart(_ context.Context, partNumber int, data io.Reader, size int64) (*upload.PartInfo, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	n, err := io.Copy(&u.buf, data)
	if err != nil {
		return nil, err
	}
	return &upload.PartInfo{PartNumber: partNumber, ETag: fmt.Sprintf("part-%d", partNumber), Size: n}, nil
}

func (u *capturingUploader) Complete(context.Context, []upload.PartInfo) error {
	u.completed = true
	return nil
}

func (u *capturingUploader) Abort(context.Context) error {
	u.aborted = true
	return nil
}

func (u *capturingUploader) GeneratePresignedURL(context.Context, time.Duration) (string, error) {
	return "https://example.blob.core.windows.net/exports/file?sig=redacted", nil
}

func (u *capturingUploader) GetLocation() string {
	return "https://example.blob.core.windows.net/exports/file"
}
func (u *capturingUploader) UploadID() string { return "mock-upload" }

// mockSentinel is a RowStreamExecutor that reports the Sentinel engine, so the pipeline's
// dispatch and the Sentinel branch are both exercised.
type mockSentinel struct {
	columns   []string
	rows      [][]interface{}
	streamErr error
	connected bool
	queries   []string
}

func (m *mockSentinel) Engine() string { return query.EngineSentinelLAW }
func (m *mockSentinel) Connect(context.Context) error {
	m.connected = true
	return nil
}
func (m *mockSentinel) Close() error              { return nil }
func (m *mockSentinel) GetAWSConfig() interface{} { return nil }
func (m *mockSentinel) GetQueryColumns(context.Context, string, string) ([]string, error) {
	return m.columns, nil
}
func (m *mockSentinel) ValidateExportQuery(context.Context, string) error  { return nil }
func (m *mockSentinel) CancelQueryExecution(context.Context, string) error { return nil }

func (m *mockSentinel) StreamRows(_ context.Context, kql string, opts query.StreamRowsOptions, fn func([]interface{}) error) (int64, error) {
	m.queries = append(m.queries, kql)
	if opts.OnColumns != nil {
		if err := opts.OnColumns(m.columns); err != nil {
			return 0, err
		}
	}
	var n int64
	for _, row := range m.rows {
		if err := fn(row); err != nil {
			return n, err
		}
		n++
	}
	return n, m.streamErr
}

func newSentinelPipeline(t *testing.T, exec query.RowStreamExecutor, up upload.CloudUploader, cfg *models.SearchExportConfig) *Pipeline {
	t.Helper()
	return New(PipelineConfig{MaxSegmentSizeMB: 1, PresignExpiry: time.Hour},
		testReportID, "Search Export - 2026-08-20 10:00:00", cfg, Deps{RowStream: exec, Uploader: up}, nil)
}

func TestPipelineRun_UsesSentinelBranch(t *testing.T) {
	mock := &mockSentinel{
		columns: []string{"TimeGenerated", "Account"},
		rows:    [][]interface{}{{"2026-08-20T10:00:00Z", "alice"}, {"2026-08-20T11:00:00Z", "bob"}},
	}
	up := &capturingUploader{}
	p := newSentinelPipeline(t, mock, up, &models.SearchExportConfig{
		Query:     "SecurityEvent | take 10",
		Format:    "csv",
		TableName: "SecurityEvent",
	})

	result, err := p.Run(context.Background(), "exports", nil)
	if err != nil {
		t.Fatalf(errPipelineRun, err)
	}
	if !mock.connected {
		t.Fatal("executor was not connected")
	}
	if result.TotalRows != 2 {
		t.Fatalf(gotTotalRows, result.TotalRows)
	}
	if !up.completed || up.aborted {
		t.Fatalf("upload completed=%v aborted=%v", up.completed, up.aborted)
	}
	if result.PresignedURL == "" {
		t.Fatal("expected a download link")
	}
	// The planned KQL runs verbatim — the worker never rewrites it.
	if len(mock.queries) != 1 || mock.queries[0] != "SecurityEvent | take 10" {
		t.Fatalf("queries = %v", mock.queries)
	}

	got := up.buf.String()
	wantLines := []string{"TimeGenerated,Account", "2026-08-20T10:00:00Z,alice", "2026-08-20T11:00:00Z,bob"}
	for _, want := range wantLines {
		if !strings.Contains(got, want) {
			t.Fatalf("csv %q missing %q", got, want)
		}
	}
	if !strings.HasPrefix(up.blobName, "exports/report-1/SearchExport_SecurityEvent_") {
		t.Fatalf("blobName = %q", up.blobName)
	}
}

// dynamic columns must reach CSV as their JSON text and NDJSON as nested values.
func TestPipelineSentinelExport_DynamicColumns(t *testing.T) {
	rows := [][]interface{}{{"alice", json.RawMessage(`{"ip":"10.0.0.1"}`)}}

	t.Run("csv", func(t *testing.T) {
		up := &capturingUploader{}
		p := newSentinelPipeline(t, &mockSentinel{columns: []string{"Account", "Props"}, rows: rows}, up,
			&models.SearchExportConfig{Query: "SecurityEvent", Format: "csv"})
		if _, err := p.Run(context.Background(), "exports", nil); err != nil {
			t.Fatalf(errPipelineRun, err)
		}
		if !strings.Contains(up.buf.String(), `"{""ip"":""10.0.0.1""}"`) {
			t.Fatalf("csv = %q, want the dynamic column as JSON text", up.buf.String())
		}
	})

	t.Run("json", func(t *testing.T) {
		up := &capturingUploader{}
		p := newSentinelPipeline(t, &mockSentinel{columns: []string{"Account", "Props"}, rows: rows}, up,
			&models.SearchExportConfig{Query: "SecurityEvent", Format: "json"})
		if _, err := p.Run(context.Background(), "exports", nil); err != nil {
			t.Fatalf(errPipelineRun, err)
		}
		var record map[string]interface{}
		if err := json.Unmarshal(bytes.TrimSpace(up.buf.Bytes()), &record); err != nil {
			t.Fatalf("ndjson: %v (%q)", err, up.buf.String())
		}
		props, ok := record["Props"].(map[string]interface{})
		if !ok || props["ip"] != "10.0.0.1" {
			t.Fatalf("Props = %#v, want a nested object", record["Props"])
		}
	})
}

// An empty range completes with a header-only file rather than failing.
func TestPipelineSentinelExport_EmptyResult(t *testing.T) {
	up := &capturingUploader{}
	p := newSentinelPipeline(t, &mockSentinel{columns: []string{"Account"}}, up,
		&models.SearchExportConfig{Query: "SecurityEvent", Format: "csv"})

	result, err := p.Run(context.Background(), "exports", nil)
	if err != nil {
		t.Fatalf(errPipelineRun, err)
	}
	if result.TotalRows != 0 {
		t.Fatalf(gotTotalRows, result.TotalRows)
	}
	if strings.TrimSpace(up.buf.String()) != "Account" {
		t.Fatalf("expected a header-only export, got %q", up.buf.String())
	}
}

// A failed query — including the partial-result case — must abort the upload so no
// truncated file is published.
func TestPipelineSentinelExport_StreamErrorAbortsUpload(t *testing.T) {
	up := &capturingUploader{}
	mock := &mockSentinel{
		columns:   []string{"Account"},
		rows:      [][]interface{}{{"alice"}},
		streamErr: fmt.Errorf("Log Analytics returned partial results after 1 rows"),
	}
	p := newSentinelPipeline(t, mock, up, &models.SearchExportConfig{Query: "SecurityEvent", Format: "csv"})

	if _, err := p.Run(context.Background(), "exports", nil); err == nil {
		t.Fatal("expected the export to fail")
	}
	if !up.aborted {
		t.Fatal("upload should have been aborted")
	}
	if up.completed {
		t.Fatal("a failed export must not complete the upload")
	}
}

// Sentinel has no server-side operation, so a stale PROCESSING report restarts.
func TestPipelineResumeRun_SentinelRestarts(t *testing.T) {
	mock := &mockSentinel{columns: []string{"Account"}, rows: [][]interface{}{{"alice"}}}
	up := &capturingUploader{}
	p := newSentinelPipeline(t, mock, up, &models.SearchExportConfig{Query: "SecurityEvent", Format: "csv"})

	result, err := p.ResumeRun(context.Background(), "exports", "stale-execution-id", nil)
	if err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	if result.TotalRows != 1 {
		t.Fatalf(gotTotalRows, result.TotalRows)
	}
	if len(mock.queries) != 1 {
		t.Fatalf("expected the query to be re-run, got %d executions", len(mock.queries))
	}
}
