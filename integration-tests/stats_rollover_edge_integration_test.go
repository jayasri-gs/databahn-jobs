//go:build integration

package integrationtests

import (
	"context"
	"testing"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/stats"
)

func TestStatsRolloverFreeformEdgeTemplateMapping(t *testing.T) {
	ctx := context.Background()
	openSearch := JobPramaan().GetOpenSearch(t)
	fixture := newStatsRolloverFixture()

	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, fixture.index)
	})

	indexStatsDocuments(t, ctx, openSearch, fixture.index, []map[string]any{
		statsEdgeDocBody(fixture, statsMetricDelivered, statsEdgeDocOpts{
			operatorID:   "parse-abc",
			targetNodeID: "filter-xyz",
			routeKey:     "onSuccess",
		}, 1, fixture.tsWin),
	})

	for _, field := range []string{"target_node_id", "route_key", "operator_id", "rule_id"} {
		assertStatsIndexMapsEdgeTagKeyword(t, ctx, openSearch, fixture.index, field)
	}
}

func TestStatsRolloverPreservesFreeformEdgeCounts(t *testing.T) {
	ctx := context.Background()
	openSearch := JobPramaan().GetOpenSearch(t)
	fixture := newStatsRolloverFixture()

	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, fixture.index)
	})

	indexStatsDocuments(t, ctx, openSearch, fixture.index, []map[string]any{
		statsEdgeDocBody(fixture, statsMetricDelivered, statsEdgeDocOpts{
			operatorID:   "parse-abc",
			targetNodeID: "filter-xyz",
			routeKey:     "onSuccess",
		}, 90, fixture.tsWin+10*60*1000),
		statsEdgeDocBody(fixture, statsMetricDelivered, statsEdgeDocOpts{
			operatorID:   "parse-abc",
			targetNodeID: "drop-node",
			routeKey:     "onFailure",
		}, 30, fixture.tsWin+20*60*1000),
		statsEdgeDocBody(fixture, statsMetricDelivered, statsEdgeDocOpts{
			operatorID:   "filter-xyz",
			targetNodeID: "sandbox-1",
			routeKey:     "onSuccess",
			ruleID:       "rule-uuid-1",
		}, 30, fixture.tsWin+25*60*1000),
		statsEdgeDocBody(fixture, statsMetricDelivered, statsEdgeDocOpts{
			operatorID:   "source-1",
			targetNodeID: "parse-abc",
			routeKey:     "onSuccess",
		}, 120, fixture.tsWin+5*60*1000),
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

	totals := sumStatsByEdgeMetric(t, ctx, openSearch, newIndexName)
	assertStatsEdgeTotal(t, totals, statsMetricDelivered, "parse-abc", "filter-xyz", "onSuccess", "", 90)
	assertStatsEdgeTotal(t, totals, statsMetricDelivered, "parse-abc", "drop-node", "onFailure", "", 30)
	assertStatsEdgeTotal(t, totals, statsMetricDelivered, "filter-xyz", "sandbox-1", "onSuccess", "rule-uuid-1", 30)
	assertStatsEdgeTotal(t, totals, statsMetricDelivered, "source-1", "parse-abc", "onSuccess", "", 120)
}

func TestStatsRolloverStampsMissingRuleIdAsNAOnRolledDocs(t *testing.T) {
	ctx := context.Background()
	openSearch := JobPramaan().GetOpenSearch(t)
	fixture := newStatsRolloverFixture()

	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, fixture.index)
	})

	indexStatsDocuments(t, ctx, openSearch, fixture.index, []map[string]any{
		statsEdgeDocBody(fixture, statsMetricDelivered, statsEdgeDocOpts{
			operatorID:   "parse-abc",
			targetNodeID: "filter-xyz",
			routeKey:     "onSuccess",
		}, 42, fixture.tsWin+15*60*1000),
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

	assertRolledStatsDocTag(t, ctx, openSearch, newIndexName, "parse-abc", "filter-xyz", "onSuccess", "N/A")
}

func TestStatsRolloverKeepsLegacyStatsWithoutEdgeDimsSeparate(t *testing.T) {
	ctx := context.Background()
	openSearch := JobPramaan().GetOpenSearch(t)
	fixture := newStatsRolloverFixture()

	t.Cleanup(func() {
		deleteStatsTestIndex(t, ctx, openSearch, fixture.index)
	})

	indexStatsDocuments(t, ctx, openSearch, fixture.index, []map[string]any{
		statsDocBody(fixture, statsMetricDelivered, "", 25, fixture.tsWin+10*60*1000),
		statsEdgeDocBody(fixture, statsMetricDelivered, statsEdgeDocOpts{
			operatorID:   "parse-abc",
			targetNodeID: "filter-xyz",
			routeKey:     "onSuccess",
		}, 10, fixture.tsWin+20*60*1000),
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

	operatorTotals := sumStatsByMetricAndOperator(t, ctx, openSearch, newIndexName)
	assertStatsTotal(t, operatorTotals, statsMetricDelivered, "N/A", 25)
	assertStatsTotal(t, operatorTotals, statsMetricDelivered, "parse-abc", 10)

	edgeTotals := sumStatsByEdgeMetric(t, ctx, openSearch, newIndexName)
	assertStatsEdgeTotal(t, edgeTotals, statsMetricDelivered, "N/A", "N/A", "N/A", "N/A", 25)
	assertStatsEdgeTotal(t, edgeTotals, statsMetricDelivered, "parse-abc", "filter-xyz", "onSuccess", "", 10)
}
