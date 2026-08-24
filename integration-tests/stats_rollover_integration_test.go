//go:build integration

package integrationtests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/stats"
)

func TestStatsRolloverHappyPath(t *testing.T) {
	ctx := context.Background()
	openSearch := JobPramaan().GetOpenSearch(t)
	fixture := newStatsRolloverFixture()

	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, fixture.index)
	})

	// Single legacy-style doc: no operator_id, coarse metric total only.
	indexStatsDocuments(t, ctx, openSearch, fixture.index, []map[string]any{
		statsDocBody(fixture, statsMetricDelivered, "", 75, fixture.tsWin+30*60*1000),
	})

	sourceIndex := stats.Index{
		Index:  fixture.index,
		Tenant: fixture.tenantID,
		Year:   2024,
		Day:    166,
		Schema: stats.Schema_V2,
		Phase:  stats.Phase_P1,
	}
	newIndexName, err := sourceIndex.NewIndexNameForRollover(time.Hour)
	if err != nil {
		t.Fatalf("build rolled index name: %v", err)
	}
	wantRolledIndex := fmt.Sprintf("rolled_over_1h_db_statistics_%s_v2_p2_y2024_d166", fixture.tenantID)
	if newIndexName != wantRolledIndex {
		t.Fatalf("rolled index name = %q, want %q", newIndexName, wantRolledIndex)
	}
	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, newIndexName)
	})

	config, err := stats.NewIntegrationRolloverConfig(time.Hour)
	if err != nil {
		t.Fatalf("integration rollover config: %v", err)
	}
	if err := stats.RollOverIndex(ctx, sourceIndex, openSearch.GetClient(), config, newIndexName); err != nil {
		t.Fatalf("rollover index: %v", err)
	}
	refreshIndex(t, ctx, openSearch, newIndexName)

	if got := countIndexDocuments(t, ctx, openSearch, newIndexName); got == 0 {
		t.Fatalf("rolled index %s has no documents after rollover", newIndexName)
	}

	totals := sumStatsByMetric(t, ctx, openSearch, newIndexName)
	assertMetricTotal(t, totals, statsMetricDelivered, 75)
}

func TestStatsRolloverPreservesPerOperatorCounts(t *testing.T) {
	ctx := context.Background()
	openSearch := JobPramaan().GetOpenSearch(t)
	fixture := newStatsRolloverFixture()

	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, fixture.index)
	})

	// Two operators in the same hour bucket for delivered; DLQ only on parse-abc.
	indexStatsDocuments(t, ctx, openSearch, fixture.index, []map[string]any{
		statsDocBody(fixture, statsMetricDelivered, "parse-abc", 100, fixture.tsWin+15*60*1000),
		statsDocBody(fixture, statsMetricDelivered, "transform-xyz", 50, fixture.tsWin+45*60*1000),
		statsDocBody(fixture, statsMetricDLQ, "parse-abc", 5, fixture.tsWin+20*60*1000),
	})

	sourceIndex := stats.Index{
		Index:  fixture.index,
		Tenant: fixture.tenantID,
		Year:   2024,
		Day:    166,
		Schema: stats.Schema_V2,
		Phase:  stats.Phase_P1,
	}
	newIndexName, err := sourceIndex.NewIndexNameForRollover(time.Hour)
	if err != nil {
		t.Fatalf("build rolled index name: %v", err)
	}
	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, newIndexName)
	})

	config, err := stats.NewIntegrationRolloverConfig(time.Hour)
	if err != nil {
		t.Fatalf("integration rollover config: %v", err)
	}
	if err := stats.RollOverIndex(ctx, sourceIndex, openSearch.GetClient(), config, newIndexName); err != nil {
		t.Fatalf("rollover index: %v", err)
	}
	refreshIndex(t, ctx, openSearch, newIndexName)

	if got := countIndexDocuments(t, ctx, openSearch, newIndexName); got == 0 {
		t.Fatalf("rolled index %s has no documents after rollover", newIndexName)
	}

	totals := sumStatsByMetricAndOperator(t, ctx, openSearch, newIndexName)
	assertStatsTotal(t, totals, statsMetricDelivered, "parse-abc", 100)
	assertStatsTotal(t, totals, statsMetricDelivered, "transform-xyz", 50)
	assertStatsTotal(t, totals, statsMetricDLQ, "parse-abc", 5)

	if _, ok := totals[statsOperatorMetric{metricName: statsMetricDelivered, operatorID: "transform-xyz"}]; !ok {
		t.Fatalf("expected transform-xyz delivered bucket to exist")
	}
	if got := totals[statsOperatorMetric{metricName: statsMetricDLQ, operatorID: "transform-xyz"}]; got != 0 {
		t.Fatalf("transform-xyz dlq total = %v, want 0", got)
	}
}

func TestStatsRolloverKeepsLegacyStatsWithoutOperatorIdSeparate(t *testing.T) {
	ctx := context.Background()
	openSearch := JobPramaan().GetOpenSearch(t)
	fixture := newStatsRolloverFixture()

	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, fixture.index)
	})

	indexStatsDocuments(t, ctx, openSearch, fixture.index, []map[string]any{
		statsDocBody(fixture, statsMetricDelivered, "", 25, fixture.tsWin+10*60*1000),
		statsDocBody(fixture, statsMetricDelivered, "parse-abc", 10, fixture.tsWin+20*60*1000),
	})

	sourceIndex := stats.Index{
		Index:  fixture.index,
		Tenant: fixture.tenantID,
		Year:   2024,
		Day:    166,
		Schema: stats.Schema_V2,
		Phase:  stats.Phase_P1,
	}
	newIndexName, err := sourceIndex.NewIndexNameForRollover(time.Hour)
	if err != nil {
		t.Fatalf("build rolled index name: %v", err)
	}
	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, newIndexName)
	})

	config, err := stats.NewIntegrationRolloverConfig(time.Hour)
	if err != nil {
		t.Fatalf("integration rollover config: %v", err)
	}
	if err := stats.RollOverIndex(ctx, sourceIndex, openSearch.GetClient(), config, newIndexName); err != nil {
		t.Fatalf("rollover index: %v", err)
	}
	refreshIndex(t, ctx, openSearch, newIndexName)

	if got := countIndexDocuments(t, ctx, openSearch, newIndexName); got == 0 {
		t.Fatalf("rolled index %s has no documents after rollover", newIndexName)
	}

	totals := sumStatsByMetricAndOperator(t, ctx, openSearch, newIndexName)
	assertStatsTotal(t, totals, statsMetricDelivered, "N/A", 25)
	assertStatsTotal(t, totals, statsMetricDelivered, "parse-abc", 10)
}
