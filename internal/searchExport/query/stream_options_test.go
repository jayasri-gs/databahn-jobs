package query

import (
	"testing"
	"time"
)

func TestStreamRowsOptionsFromEnv(t *testing.T) {
	t.Setenv("SEARCH_EXPORT_MAX_ROWS", "5000")
	t.Setenv("SEARCH_EXPORT_SYNAPSE_QUERY_TIMEOUT_MINUTES", "45")
	opts := StreamRowsOptionsFromEnv()
	if opts.MaxRows != 5000 {
		t.Fatalf("MaxRows=%d", opts.MaxRows)
	}
	if opts.QueryTimeout != 45*time.Minute {
		t.Fatalf("QueryTimeout=%v", opts.QueryTimeout)
	}
}
