package pipeline

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"go.uber.org/zap"
)

type mockUploader struct{}

func (m *mockUploader) Init(context.Context, string, string, string) error { return nil }
func (m *mockUploader) UploadPart(_ context.Context, partNumber int, _ io.Reader, size int64) (*upload.PartInfo, error) {
	return &upload.PartInfo{PartNumber: partNumber, ETag: "etag", Size: size}, nil
}
func (m *mockUploader) Complete(context.Context, []upload.PartInfo) error { return nil }
func (m *mockUploader) Abort(context.Context) error                       { return nil }
func (m *mockUploader) GeneratePresignedURL(context.Context, time.Duration) (string, error) {
	return "https://example/presigned", nil
}
func (m *mockUploader) GetLocation() string { return "mock://location" }
func (m *mockUploader) UploadID() string    { return "mock-upload" }

type mockAthena struct {
	status     string
	statusErr  error
	result     *query.UnloadResult
	execCalled bool
	waitCalled bool
}

func (m *mockAthena) Engine() string                { return query.EngineAthena }
func (m *mockAthena) Connect(context.Context) error { return nil }
func (m *mockAthena) Close() error                  { return nil }
func (m *mockAthena) GetQueryColumns(context.Context, string, string) ([]string, error) {
	return []string{"col"}, nil
}
func (m *mockAthena) GetAWSConfig() interface{} { return nil }
func (m *mockAthena) ExecuteUnloadAsync(context.Context, string, string, string, query.UnloadOptions) (string, error) {
	m.execCalled = true
	return "fresh-exec-id", nil
}
func (m *mockAthena) CheckQueryStatus(context.Context, string) (string, error) {
	return m.status, m.statusErr
}
func (m *mockAthena) WaitForExecution(context.Context, string) error {
	m.waitCalled = true
	return nil
}
func (m *mockAthena) GetExecutionResult(context.Context, string) (*query.UnloadResult, error) {
	return m.result, nil
}
func (m *mockAthena) CancelQueryExecution(context.Context, string) error { return nil }
func (m *mockAthena) GetOutputLocation() string                          { return "s3://staging/out/" }
func (m *mockAthena) NewStagingReader(string) (unload.StagingReader, error) {
	return &mockStagingReader{}, nil
}

func newTestPipeline(athena *mockAthena) *Pipeline {
	return New(PipelineConfig{TempDir: "/tmp"}, "report-1", "Search Export - 2026-07-15 10:00:00",
		&models.SearchExportConfig{Query: "SELECT 1", Format: "csv"}, athena, nil, &mockUploader{}, nil, zap.NewNop())
}

func TestResumeRun_EmptyExecutionID_RunsFresh(t *testing.T) {
	athena := &mockAthena{result: &query.UnloadResult{OutputLocation: "out/"}}
	if _, err := newTestPipeline(athena).ResumeRun(context.Background(), "dest", "", nil); err != nil {
		t.Fatal(err)
	}
	if !athena.execCalled {
		t.Error("expected fresh UNLOAD when no execution ID recorded")
	}
}

func TestResumeRun_FailedStatus_RunsFresh(t *testing.T) {
	athena := &mockAthena{status: query.QueryStateFailed, result: &query.UnloadResult{OutputLocation: "out/"}}
	if _, err := newTestPipeline(athena).ResumeRun(context.Background(), "dest", "old-exec", nil); err != nil {
		t.Fatal(err)
	}
	if !athena.execCalled {
		t.Error("expected fresh UNLOAD for FAILED query status")
	}
}

func TestResumeRun_Succeeded_SkipsQueryAndUploads(t *testing.T) {
	athena := &mockAthena{status: query.QueryStateSucceeded, result: &query.UnloadResult{OutputLocation: "out/"}}
	res, err := newTestPipeline(athena).ResumeRun(context.Background(), "dest", "old-exec", nil)
	if err != nil {
		t.Fatal(err)
	}
	if athena.execCalled {
		t.Error("must not re-run UNLOAD when query already succeeded")
	}
	if res == nil {
		t.Fatal("expected result")
	}
}

func TestResumeRun_Running_WaitsThenUploads(t *testing.T) {
	athena := &mockAthena{status: query.QueryStateRunning, result: &query.UnloadResult{OutputLocation: "out/"}}
	if _, err := newTestPipeline(athena).ResumeRun(context.Background(), "dest", "old-exec", nil); err != nil {
		t.Fatal(err)
	}
	if !athena.waitCalled {
		t.Error("expected WaitForExecution for RUNNING query")
	}
	if athena.execCalled {
		t.Error("must not re-run UNLOAD when reattaching to a running query")
	}
}

func TestResumeRun_NoOutputLocation_RunsFresh(t *testing.T) {
	athena := &mockAthena{status: query.QueryStateSucceeded, result: &query.UnloadResult{}}
	if _, err := newTestPipeline(athena).ResumeRun(context.Background(), "dest", "old-exec", nil); err != nil {
		t.Fatal(err)
	}
	if !athena.execCalled {
		t.Error("expected fresh UNLOAD when reattached result has no output location")
	}
}
