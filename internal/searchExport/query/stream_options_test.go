package query

import (
	"testing"
	"time"
)

func TestStreamRowsOptionsFromEnv_DefaultSkipPreflight(t *testing.T) {
	opts := StreamRowsOptionsFromEnv()
	if !opts.SkipPreflight {
		t.Fatal("expected SkipPreflight by default for single-query stream export")
	}
}

func TestStreamRowsOptionsFromEnv_CanDisableSkipPreflight(t *testing.T) {
	t.Setenv("SEARCH_EXPORT_SYNAPSE_SKIP_PREFLIGHT", "false")
	opts := StreamRowsOptionsFromEnv()
	if opts.SkipPreflight {
		t.Fatal("expected SkipPreflight=false when env explicitly false")
	}
}

func TestStreamRowsOptionsFromEnv(t *testing.T) {
	t.Setenv("SEARCH_EXPORT_MAX_ROWS", "5000")
	t.Setenv("SEARCH_EXPORT_SYNAPSE_QUERY_TIMEOUT_MINUTES", "45")
	t.Setenv("SEARCH_EXPORT_SYNAPSE_SKIP_PREFLIGHT", "true")
	t.Setenv("SEARCH_EXPORT_SYNAPSE_PROGRESS_EVERY_ROWS", "50")
	opts := StreamRowsOptionsFromEnv()
	if opts.MaxRows != 5000 {
		t.Fatalf("MaxRows=%d", opts.MaxRows)
	}
	if opts.ServerRowCap != 5000 {
		t.Fatalf("ServerRowCap=%d", opts.ServerRowCap)
	}
	if opts.QueryTimeout != 45*time.Minute {
		t.Fatalf("QueryTimeout=%v", opts.QueryTimeout)
	}
	if !opts.SkipPreflight {
		t.Fatal("expected SkipPreflight")
	}
	if opts.ProgressEveryRows != 50 {
		t.Fatalf("ProgressEveryRows=%d", opts.ProgressEveryRows)
	}
}

func TestStreamRowsOptionsFromEnv_MaxRowsEnablesSkipPreflight(t *testing.T) {
	t.Setenv("SEARCH_EXPORT_MAX_ROWS", "50")
	t.Setenv("SEARCH_EXPORT_SYNAPSE_SKIP_PREFLIGHT", "false")
	opts := StreamRowsOptionsFromEnv()
	if !opts.SkipPreflight {
		t.Fatal("expected SkipPreflight when SEARCH_EXPORT_MAX_ROWS is set")
	}
	if opts.ServerRowCap != 50 {
		t.Fatalf("ServerRowCap=%d", opts.ServerRowCap)
	}
}
