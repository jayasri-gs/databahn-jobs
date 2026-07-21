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

func TestHourPartitionFilter_external(t *testing.T) {
	ms := time.Date(2026, 6, 18, 9, 0, 0, 0, time.UTC).UnixMilli()
	got := HourPartitionFilter(ms, ExternalHivePartitionColumns)
	// Aliases must match the backend Synapse view projection: year/month/day/hour.
	want := "year = '2026' AND month = '06' AND day = '18' AND hour = '09'"
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

func TestRangePartitionFilter_EndOnHourBoundary_IncludesBoundaryHour(t *testing.T) {
	// End exactly at 11:00:00.000 — rows with db_edge_ts == end live in hour
	// partition 11 and must not be dropped.
	startMs := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC).UnixMilli()
	endMs := time.Date(2026, 7, 15, 11, 0, 0, 0, time.UTC).UnixMilli()
	got := RangePartitionFilter(startMs, endMs, DestinationPartitionColumns)
	if !strings.Contains(got, "hour_partition = '11'") {
		t.Errorf("filter must include boundary hour 11:\n%s", got)
	}
}

func TestPlanHourChunks_InclusiveEndBoundary(t *testing.T) {
	start := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC).UnixMilli()
	end := time.Date(2026, 7, 15, 11, 0, 0, 0, time.UTC).UnixMilli()
	chunks := PlanHourChunks(start, end)
	if len(chunks) != 2 {
		t.Fatalf("chunks=%d want 2 (hours 10 and 11, end inclusive)", len(chunks))
	}
	if chunks[1].StartMs != end {
		t.Errorf("second chunk start=%d want %d", chunks[1].StartMs, end)
	}
}

func TestRangePartitionFilter_HugeRange_FallsBackToBoundsOnly(t *testing.T) {
	// 1 year ≈ 8760 hours — far above the predicate cap. Filter must stay small:
	// db_edge_ts bounds only, no per-hour partition predicates.
	startMs := time.Date(2025, 7, 15, 0, 0, 0, 0, time.UTC).UnixMilli()
	endMs := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC).UnixMilli()
	got := RangePartitionFilter(startMs, endMs, DestinationPartitionColumns)
	if got == "" {
		t.Fatal("expected non-empty filter")
	}
	if strings.Contains(got, "hour_partition") {
		t.Errorf("huge range must not enumerate hour predicates, got %d bytes", len(got))
	}
	for _, want := range []string{
		fmt.Sprintf("db_edge_ts >= '%d'", startMs),
		fmt.Sprintf("db_edge_ts <= '%d'", endMs),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("filter missing %q", want)
		}
	}
}
