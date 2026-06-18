package query

import (
	"strings"
	"testing"
)

func TestStripTop_removesLimit(t *testing.T) {
	stripped, limit := stripTop("SELECT TOP 100 * FROM t")
	if limit != 100 {
		t.Fatalf("limit=%d", limit)
	}
	if strings.Contains(strings.ToUpper(stripped), "TOP") {
		t.Fatalf("top not stripped: %q", stripped)
	}
}

func TestBuildSubChunkQuery_replacesExistingTop(t *testing.T) {
	got := BuildSubChunkQuery("SELECT TOP 100 * FROM t WHERE x = 1", 50_000, "")
	if strings.Count(strings.ToUpper(got), "TOP") != 1 {
		t.Fatalf("expected single TOP, got %q", got)
	}
	if !strings.Contains(got, "TOP (100)") {
		t.Fatalf("expected TOP (100), got %q", got)
	}
}

func TestBuildSubChunkQuery_batchSmallerThanExportLimit(t *testing.T) {
	got := BuildSubChunkQuery("SELECT TOP 1000 * FROM t", 50, "")
	if !strings.Contains(got, "TOP (50)") {
		t.Fatalf("got %q", got)
	}
}

func TestBuildSubChunkQuery_keyset(t *testing.T) {
	got := BuildSubChunkQuery("SELECT * FROM t WHERE x = 1", 100, "1700000000000")
	for _, want := range []string{"TOP (100)", "db_edge_ts > '1700000000000'", "ORDER BY db_edge_ts"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %q", want, got)
		}
	}
}

func TestEffectiveTopLimit(t *testing.T) {
	if got := effectiveTopLimit(50_000, 100); got != 100 {
		t.Fatalf("got %d", got)
	}
	if got := effectiveTopLimit(50, 100); got != 50 {
		t.Fatalf("got %d", got)
	}
}

func TestBuildSubChunkQuery_multilineOrderBy(t *testing.T) {
	got := BuildSubChunkQuery("SELECT * FROM t\nORDER BY db_edge_ts", 100, "")
	if strings.Count(strings.ToUpper(got), "ORDER BY") != 1 {
		t.Fatalf("duplicate ORDER BY: %q", got)
	}
}

func TestStripTop_ignoresNestedSelect(t *testing.T) {
	sql := "SELECT * FROM (SELECT TOP 100 * FROM inner_t) s"
	stripped, limit := stripTop(sql)
	if limit != 0 {
		t.Fatalf("limit=%d want 0", limit)
	}
	if !strings.Contains(strings.ToUpper(stripped), "TOP 100") {
		t.Fatalf("inner TOP stripped: %q", stripped)
	}
}

func TestInjectTop_cteOuterSelect(t *testing.T) {
	sql := "WITH cte AS (SELECT col FROM t) SELECT * FROM cte"
	got := injectTop(sql, 50)
	if !strings.HasPrefix(strings.ToUpper(got), "WITH CTE AS") {
		t.Fatalf("unexpected: %q", got)
	}
	if !strings.Contains(got, "TOP (50) * FROM cte") {
		t.Fatalf("TOP not on outer select: %q", got)
	}
}
