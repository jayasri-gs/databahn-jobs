package query

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPlanHourChunks_oneDay(t *testing.T) {
	start := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC).UnixMilli()
	end := time.Date(2026, 6, 18, 23, 59, 59, 0, time.UTC).UnixMilli()
	chunks := PlanHourChunks(start, end)
	if len(chunks) != 24 {
		t.Fatalf("chunks=%d want 24", len(chunks))
	}
}

func TestPlanHourChunks_partialRange(t *testing.T) {
	start := time.Date(2026, 6, 18, 10, 30, 0, 0, time.UTC).UnixMilli()
	end := time.Date(2026, 6, 18, 14, 45, 0, 0, time.UTC).UnixMilli()
	chunks := PlanHourChunks(start, end)
	if len(chunks) != 5 {
		t.Fatalf("chunks=%d want 5", len(chunks))
	}
}

func TestHourPartitionFilter_destination(t *testing.T) {
	ms := time.Date(2026, 6, 18, 9, 0, 0, 0, time.UTC).UnixMilli()
	got := HourPartitionFilter(ms, DestinationPartitionColumns)
	want := "year_partition = '2026' AND month_partition = '06' AND day_partition = '18' AND hour_partition = '09'"
	if got != want {
		t.Fatalf("got %q", got)
	}
}

func TestHourChunkFilter_firstHourMinBound(t *testing.T) {
	rangeStart := time.Date(2026, 6, 18, 10, 30, 0, 0, time.UTC).UnixMilli()
	rangeEnd := time.Date(2026, 6, 18, 14, 45, 0, 0, time.UTC).UnixMilli()
	chunkStart := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC).UnixMilli()
	got := HourChunkFilter(chunkStart, rangeStart, rangeEnd, DestinationPartitionColumns)
	if !strings.Contains(got, "hour_partition = '10'") {
		t.Fatalf("missing hour partition: %q", got)
	}
	if !strings.Contains(got, "db_edge_ts >= '") {
		t.Fatalf("missing min bound: %q", got)
	}
}

func TestHourChunkFilter_lastHourMaxBound(t *testing.T) {
	rangeStart := time.Date(2026, 6, 18, 10, 30, 0, 0, time.UTC).UnixMilli()
	rangeEnd := time.Date(2026, 6, 18, 14, 45, 0, 0, time.UTC).UnixMilli()
	chunkStart := time.Date(2026, 6, 18, 14, 0, 0, 0, time.UTC).UnixMilli()
	got := HourChunkFilter(chunkStart, rangeStart, rangeEnd, DestinationPartitionColumns)
	if !strings.Contains(got, "hour_partition = '14'") {
		t.Fatalf("missing hour partition: %q", got)
	}
	if !strings.Contains(got, "db_edge_ts <= '") {
		t.Fatalf("missing max bound: %q", got)
	}
}

func TestHourChunkFilter_middleHourNoEdgeBounds(t *testing.T) {
	rangeStart := time.Date(2026, 6, 18, 10, 30, 0, 0, time.UTC).UnixMilli()
	rangeEnd := time.Date(2026, 6, 18, 14, 45, 0, 0, time.UTC).UnixMilli()
	chunkStart := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC).UnixMilli()
	got := HourChunkFilter(chunkStart, rangeStart, rangeEnd, DestinationPartitionColumns)
	if strings.Contains(got, "db_edge_ts") {
		t.Fatalf("unexpected edge bound: %q", got)
	}
}

func TestRangePartitionFilter_TwoHours(t *testing.T) {
	// 2026-07-15 10:30:00 UTC .. 2026-07-15 11:10:00 UTC → hours 10 and 11
	startMs := time.Date(2026, 7, 15, 10, 30, 0, 0, time.UTC).UnixMilli()
	endMs := time.Date(2026, 7, 15, 11, 10, 0, 0, time.UTC).UnixMilli()
	got := RangePartitionFilter(startMs, endMs, DestinationPartitionColumns)

	for _, want := range []string{
		"hour_partition = '10'",
		"hour_partition = '11'",
		" OR ",
		fmt.Sprintf("db_edge_ts >= '%d'", startMs),
		fmt.Sprintf("db_edge_ts <= '%d'", endMs),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("filter missing %q:\n%s", want, got)
		}
	}
}

func TestRangePartitionFilter_InvalidRange(t *testing.T) {
	if got := RangePartitionFilter(100, 50, DestinationPartitionColumns); got != "" {
		t.Errorf("expected empty filter for inverted range, got %q", got)
	}
}
