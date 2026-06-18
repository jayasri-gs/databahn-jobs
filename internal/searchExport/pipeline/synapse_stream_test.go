package pipeline

import (
	"context"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
)

type mockSynapseStream struct {
	rows [][]interface{}
}

func (m *mockSynapseStream) Engine() string                { return query.EngineSynapse }
func (m *mockSynapseStream) Connect(context.Context) error { return nil }
func (m *mockSynapseStream) Close() error                  { return nil }
func (m *mockSynapseStream) GetAWSConfig() interface{}     { return nil }
func (m *mockSynapseStream) GetQueryColumns(context.Context, string, string) ([]string, error) {
	return []string{"col1"}, nil
}
func (m *mockSynapseStream) ValidateExportQuery(context.Context, string) error { return nil }
func (m *mockSynapseStream) CancelQueryExecution(context.Context, string) error {
	return nil
}
func (m *mockSynapseStream) StreamRows(_ context.Context, _ string, _ query.StreamRowsOptions, fn func([]interface{}) error) (int64, error) {
	var n int64
	for _, r := range m.rows {
		if err := fn(r); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func TestColumnIndex_caseInsensitive(t *testing.T) {
	cols := []string{"DB_EDGE_TS", "col1"}
	if got := columnIndex(cols, "db_edge_ts"); got != 0 {
		t.Fatalf("columnIndex = %d, want 0", got)
	}
}

func TestPipelineRun_UsesSynapseBranch(t *testing.T) {
	mock := &mockSynapseStream{rows: [][]interface{}{{"a"}}}
	p := New(PipelineConfig{MaxSegmentSizeMB: 1}, "report-1", "test_npe 2026-06-17", &models.SearchExportConfig{
		Query:  "SELECT col1 FROM t",
		Format: "csv",
	}, nil, mock, nil, nil)

	if p.synapse == nil {
		t.Fatal("expected synapse executor")
	}
	if p.athena != nil {
		t.Fatal("athena should be nil for synapse pipeline")
	}
}
