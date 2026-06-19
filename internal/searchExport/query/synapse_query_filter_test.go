package query

import (
	"strings"
	"testing"
)

func TestWrapWithPartitionFilter_orderBy(t *testing.T) {
	got := WrapWithPartitionFilter("SELECT * FROM t ORDER BY ts", "year_partition = '2026'")
	if !strings.HasPrefix(got, "SELECT * FROM (\nSELECT * FROM t ORDER BY ts\n) AS _q WHERE") {
		t.Fatalf("ORDER BY not wrapped correctly: %q", got)
	}
}

func TestWrapWithPartitionFilter_semicolon(t *testing.T) {
	got := WrapWithPartitionFilter("SELECT * FROM t;", "year_partition = '2026'")
	if strings.Contains(got, ";") {
		t.Fatalf("trailing semicolon not stripped: %q", got)
	}
}

func TestWrapWithPartitionFilter_emptyFilter(t *testing.T) {
	q := "SELECT * FROM t ORDER BY ts"
	if got := WrapWithPartitionFilter(q, ""); got != q {
		t.Fatalf("empty filter should return query unchanged, got %q", got)
	}
}

func TestAddPartitionFilter_withWhere(t *testing.T) {
	got := AddPartitionFilter("SELECT * FROM t WHERE a = 1", "day_partition = '18'")
	if got != "SELECT * FROM t WHERE a = 1 AND (day_partition = '18')" {
		t.Fatalf("got %q", got)
	}
}

func TestAddPartitionFilter_multilineWhere(t *testing.T) {
	got := AddPartitionFilter("SELECT * FROM t\nWHERE a = 1", "day_partition = '18'")
	want := "SELECT * FROM t\nWHERE a = 1 AND (day_partition = '18')"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
