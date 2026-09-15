package query

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.uber.org/zap"
)

type mockSplunkTransport struct {
	export func(context.Context, string, func([]string) error, func([]interface{}) error) (int64, SplunkStreamMetadata, error)
}

func (m *mockSplunkTransport) Connect(context.Context) error { return nil }

func (m *mockSplunkTransport) Export(
	ctx context.Context,
	spl string,
	onColumns func([]string) error,
	onRow func([]interface{}) error,
) (int64, SplunkStreamMetadata, error) {
	if m.export == nil {
		return 0, SplunkStreamMetadata{}, nil
	}
	return m.export(ctx, spl, onColumns, onRow)
}

func newTestSplunkExecutor(transport *mockSplunkTransport) *SplunkExecutor {
	exec := &SplunkExecutor{transport: transport}
	exec.SetLogger(zap.NewNop())
	return exec
}

func TestIsSplunkAggregationPipeline(t *testing.T) {
	if !isSplunkAggregationPipeline(`search index=main | stats count by host`) {
		t.Fatal("expected stats pipeline to be aggregation")
	}
	if isSplunkAggregationPipeline(`search index=main | head 1000`) {
		t.Fatal("expected head-only pipeline not to be aggregation")
	}
}

func TestParseSplunkHeadLimit(t *testing.T) {
	limit, ok := parseSplunkHeadLimit(`search index=main earliest=-1d | head 1000000`)
	if !ok || limit != 1_000_000 {
		t.Fatalf("head limit = %d, ok=%v", limit, ok)
	}
}

func TestReconcileSplunkExportTruncationEmptyResult(t *testing.T) {
	if err := reconcileSplunkExportTruncation(`search index=main | head 10`, 0, SplunkStreamMetadata{}); err != nil {
		t.Fatalf("empty export should succeed: %v", err)
	}
}

func TestReconcileSplunkExportTruncationPrefersMessages(t *testing.T) {
	meta := SplunkStreamMetadata{
		TruncationHints: []string{"Result set truncated at 50000 rows"},
	}
	err := reconcileSplunkExportTruncation(`search index=main | stats count by host`, 123, meta)
	if err == nil || !strings.Contains(err.Error(), "truncated at 50000") {
		t.Fatalf("err = %v", err)
	}
}

func TestReconcileSplunkExportTruncationAggregationAtLimit(t *testing.T) {
	spl := `search index=main | stats count by host`
	err := reconcileSplunkExportTruncation(spl, defaultSplunkMaxResultRows, SplunkStreamMetadata{})
	if err == nil || !strings.Contains(err.Error(), "maxresultrows") {
		t.Fatalf("err = %v", err)
	}
}

func TestReconcileSplunkExportTruncationAggregationBelowLimit(t *testing.T) {
	spl := `search index=main | stats count by host`
	if err := reconcileSplunkExportTruncation(spl, defaultSplunkMaxResultRows-1, SplunkStreamMetadata{}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestReconcileSplunkExportTruncationNonAggregationExactHead(t *testing.T) {
	spl := `search index=main earliest=-1d | head 50000`
	if err := reconcileSplunkExportTruncation(spl, 50000, SplunkStreamMetadata{}); err != nil {
		t.Fatalf("exact head match should succeed: %v", err)
	}
}

func TestSplunkExecutorStreamRowsFailsAggregationTruncation(t *testing.T) {
	spl := `search index=main | stats count by host`
	exec := newTestSplunkExecutor(&mockSplunkTransport{
		export: func(_ context.Context, gotSPL string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, SplunkStreamMetadata, error) {
			if gotSPL != spl {
				t.Fatalf("spl = %q", gotSPL)
			}
			if err := onColumns([]string{"count"}); err != nil {
				return 0, SplunkStreamMetadata{}, err
			}
			for i := int64(0); i < defaultSplunkMaxResultRows; i++ {
				if err := onRow([]interface{}{i}); err != nil {
					return i, SplunkStreamMetadata{}, err
				}
			}
			return defaultSplunkMaxResultRows, SplunkStreamMetadata{}, nil
		},
	})
	_, err := exec.StreamRows(context.Background(), spl, StreamRowsOptions{}, func([]interface{}) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "maxresultrows") {
		t.Fatalf("err = %v", err)
	}
}

func TestSplunkExecutorStreamRowsAllowsExactHead(t *testing.T) {
	spl := `search index=main | head 2`
	exec := newTestSplunkExecutor(&mockSplunkTransport{
		export: func(_ context.Context, _ string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, SplunkStreamMetadata, error) {
			if err := onColumns([]string{"a"}); err != nil {
				return 0, SplunkStreamMetadata{}, err
			}
			for _, v := range []interface{}{"one", "two"} {
				if err := onRow([]interface{}{v}); err != nil {
					return 0, SplunkStreamMetadata{}, err
				}
			}
			return 2, SplunkStreamMetadata{}, nil
		},
	})
	n, err := exec.StreamRows(context.Background(), spl, StreamRowsOptions{}, func([]interface{}) error { return nil })
	if err != nil {
		t.Fatalf("StreamRows: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows = %d", n)
	}
}

func TestSplunkExecutorStreamRowsEmptySuccess(t *testing.T) {
	exec := newTestSplunkExecutor(&mockSplunkTransport{
		export: func(context.Context, string, func([]string) error, func([]interface{}) error) (int64, SplunkStreamMetadata, error) {
			return 0, SplunkStreamMetadata{}, nil
		},
	})
	n, err := exec.StreamRows(context.Background(), `search index=main | head 0`, StreamRowsOptions{}, func([]interface{}) error { return nil })
	if err != nil {
		t.Fatalf("StreamRows: %v", err)
	}
	if n != 0 {
		t.Fatalf("rows = %d", n)
	}
}

func TestSplunkExecutorStreamRowsPropagatesOnRowError(t *testing.T) {
	want := errors.New("row failed")
	exec := newTestSplunkExecutor(&mockSplunkTransport{
		export: func(_ context.Context, _ string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, SplunkStreamMetadata, error) {
			if err := onColumns([]string{"a"}); err != nil {
				return 0, SplunkStreamMetadata{}, err
			}
			if err := onRow([]interface{}{"one"}); err != nil {
				return 0, SplunkStreamMetadata{}, err
			}
			return 1, SplunkStreamMetadata{}, nil
		},
	})
	_, err := exec.StreamRows(context.Background(), `search index=main | head 1`, StreamRowsOptions{}, func([]interface{}) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}
