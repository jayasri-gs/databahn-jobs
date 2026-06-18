package query

import "testing"

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
