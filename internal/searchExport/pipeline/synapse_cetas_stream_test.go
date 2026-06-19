package pipeline

import (
	"context"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

// mockCETASExecutor implements both RowStreamExecutor and CETASExecutor.
type mockCETASExecutor struct {
	mockSynapseStream
	ddlCalls []string
}

func (m *mockCETASExecutor) ExecDDL(_ context.Context, ddl string) error {
	m.ddlCalls = append(m.ddlCalls, ddl)
	return nil
}

func (m *mockCETASExecutor) GetQueryColumns(_ context.Context, _, _ string) ([]string, error) {
	return []string{"col1"}, nil
}

func TestPipelineNew_SetsCETASExecWhenStagingConfigProvided(t *testing.T) {
	mock := &mockCETASExecutor{}
	stagingCfg := &destination.AzureBlobConfig{
		AuthType:         "AUTH_CONNECTION_STRING",
		ConnectionString: "AccountName=test;AccountKey=dGVzdA==",
		Container:        "exports",
	}
	p := New(PipelineConfig{MaxSegmentSizeMB: 1}, "report-1", "export", &models.SearchExportConfig{
		Query:  "SELECT col1 FROM t",
		Format: "csv",
	}, nil, mock, nil, stagingCfg, nil)

	if p.cetasExec == nil {
		t.Fatal("expected cetasExec to be set when synapse implements CETASExecutor and stagingBlobCfg != nil")
	}
	if p.stagingBlobCfg == nil {
		t.Fatal("expected stagingBlobCfg to be stored on pipeline")
	}
}

func TestPipelineNew_NoCETASExecWhenNoStagingConfig(t *testing.T) {
	mock := &mockCETASExecutor{}
	p := New(PipelineConfig{MaxSegmentSizeMB: 1}, "report-1", "export", &models.SearchExportConfig{
		Query:  "SELECT col1 FROM t",
		Format: "csv",
	}, nil, mock, nil, nil, nil)

	if p.cetasExec != nil {
		t.Fatal("cetasExec should be nil when stagingBlobCfg is nil")
	}
}

func TestPipelineNew_NoCETASExecForJDBCOnlyMock(t *testing.T) {
	// mockSynapseStream does NOT implement CETASExecutor (no ExecDDL method).
	// Even with a staging config, cetasExec should be nil → falls back to JDBC.
	mock := &mockSynapseStream{rows: [][]interface{}{{"a"}}}
	stagingCfg := &destination.AzureBlobConfig{Container: "exports"}
	p := New(PipelineConfig{MaxSegmentSizeMB: 1}, "report-1", "export", &models.SearchExportConfig{
		Query:  "SELECT col1 FROM t",
		Format: "csv",
	}, nil, mock, nil, stagingCfg, nil)

	if p.cetasExec != nil {
		t.Fatal("cetasExec should be nil for executor that does not implement CETASExecutor")
	}
}

// Verify *query.SynapseExecutor satisfies the CETASExecutor interface at compile time.
var _ query.CETASExecutor = (*query.SynapseExecutor)(nil)
