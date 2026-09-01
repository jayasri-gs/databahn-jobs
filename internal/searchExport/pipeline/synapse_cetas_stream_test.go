package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"go.uber.org/zap"
)

// mockCETASExecutor implements both RowStreamExecutor and CETASExecutor.
type mockCETASExecutor struct {
	mockSynapseStream
	ddls           []string
	tableExists    bool
	tableExistsErr error
}

func (m *mockCETASExecutor) ExecDDL(_ context.Context, ddl string) error {
	m.ddls = append(m.ddls, ddl)
	return nil
}

func (m *mockCETASExecutor) GetQueryColumns(_ context.Context, _, _ string) ([]string, error) {
	return []string{"col1"}, nil
}

func (m *mockCETASExecutor) ExternalTableExists(_ context.Context, _ string) (bool, error) {
	return m.tableExists, m.tableExistsErr
}

func TestPipelineNew_SetsCETASExecWhenStagingConfigProvided(t *testing.T) {
	mock := &mockCETASExecutor{}
	stagingCfg := &destination.AzureBlobConfig{
		AuthType:         "AUTH_CONNECTION_STRING",
		ConnectionString: "AccountName=test;AccountKey=dGVzdA==",
		Container:        "exports",
	}
	p := New(PipelineConfig{MaxSegmentSizeMB: 1}, testReportID, "export", &models.SearchExportConfig{
		Query:  "SELECT col1 FROM t",
		Format: "csv",
	}, Deps{RowStream: mock, StagingBlob: stagingCfg}, nil)

	if p.cetasExec == nil {
		t.Fatal("expected cetasExec to be set when synapse implements CETASExecutor and stagingBlobCfg != nil")
	}
	if p.stagingBlobCfg == nil {
		t.Fatal("expected stagingBlobCfg to be stored on pipeline")
	}
}

func TestPipelineNew_NoCETASExecWhenNoStagingConfig(t *testing.T) {
	mock := &mockCETASExecutor{}
	p := New(PipelineConfig{MaxSegmentSizeMB: 1}, testReportID, "export", &models.SearchExportConfig{
		Query:  "SELECT col1 FROM t",
		Format: "csv",
	}, Deps{RowStream: mock}, nil)

	if p.cetasExec != nil {
		t.Fatal("cetasExec should be nil when stagingBlobCfg is nil")
	}
}

func TestPipelineNew_NoCETASExecForJDBCOnlyMock(t *testing.T) {
	// mockSynapseStream does NOT implement CETASExecutor (no ExecDDL method).
	// Even with a staging config, cetasExec should be nil → falls back to JDBC.
	mock := &mockSynapseStream{rows: [][]interface{}{{"a"}}}
	stagingCfg := &destination.AzureBlobConfig{Container: "exports"}
	p := New(PipelineConfig{MaxSegmentSizeMB: 1}, testReportID, "export", &models.SearchExportConfig{
		Query:  "SELECT col1 FROM t",
		Format: "csv",
	}, Deps{RowStream: mock, StagingBlob: stagingCfg}, nil)

	if p.cetasExec != nil {
		t.Fatal("cetasExec should be nil for executor that does not implement CETASExecutor")
	}
}

// Verify *query.SynapseExecutor satisfies the CETASExecutor interface at compile time.
var _ query.CETASExecutor = (*query.SynapseExecutor)(nil)

type mockStagingReader struct {
	files   []string
	deleted [][]string
}

func (m *mockStagingReader) ListFiles(context.Context, string) ([]string, error) {
	return m.files, nil
}
func (m *mockStagingReader) ParseManifest(context.Context, string) ([]string, error) {
	return m.files, nil
}
func (m *mockStagingReader) StreamToUploader(context.Context, upload.CloudUploader, []string, []byte, string, string, *zap.Logger) (int64, int64, error) {
	return 0, 0, nil
}
func (m *mockStagingReader) StreamRows(context.Context, []string, func([]interface{}) error) error {
	return nil
}
func (m *mockStagingReader) DeleteFiles(_ context.Context, files []string) error {
	m.deleted = append(m.deleted, files)
	return nil
}
func (m *mockStagingReader) Columns() []string { return nil }

func TestPrepareCETASStaging_TableExists_SkipsQueryAndCleanup(t *testing.T) {
	mock := &mockCETASExecutor{tableExists: true}
	reader := &mockStagingReader{files: []string{"databahn_out/r/full/1.csv"}}
	runQuery, err := prepareCETASStaging(context.Background(), mock, reader, "staging_ab", "databahn_out/r/full/", zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if runQuery {
		t.Error("expected runQuery=false when table exists")
	}
	if len(reader.deleted) != 0 {
		t.Errorf("must not delete staged blobs of a completed CETAS, deleted %v", reader.deleted)
	}
}

func TestPrepareCETASStaging_TableMissing_CleansLeftovers(t *testing.T) {
	mock := &mockCETASExecutor{tableExists: false}
	reader := &mockStagingReader{files: []string{"databahn_out/r/full/partial.csv"}}
	runQuery, err := prepareCETASStaging(context.Background(), mock, reader, "staging_ab", "databahn_out/r/full/", zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if !runQuery {
		t.Error("expected runQuery=true when table missing")
	}
	if len(reader.deleted) != 1 {
		t.Errorf("expected leftover blobs deleted, deleted %v", reader.deleted)
	}
}

func TestCleanupCETASArtifacts_DropsTableBeforeDataSource(t *testing.T) {
	mock := &mockCETASExecutor{}
	reader := &mockStagingReader{files: []string{"databahn_out/rid/full/1.csv"}}
	CleanupCETASArtifacts(context.Background(), mock, reader, "rid-with-hyphens-123", zap.NewNop())

	var tableIdx, dsIdx = -1, -1
	for i, ddl := range mock.ddls {
		if strings.Contains(ddl, "DROP EXTERNAL TABLE") && tableIdx < 0 {
			tableIdx = i
		}
		if strings.Contains(ddl, "DROP EXTERNAL DATA SOURCE") && dsIdx < 0 {
			dsIdx = i
		}
	}
	if tableIdx < 0 || dsIdx < 0 || tableIdx > dsIdx {
		t.Errorf("table drop must come before data source drop, ddls=%v", mock.ddls)
	}
	if len(reader.deleted) != 1 {
		t.Errorf("expected staged blobs deleted, got %v", reader.deleted)
	}
}
