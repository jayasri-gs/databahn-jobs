//go:build integration

package integrationtests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	dbos "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/pramaan-go/pramaan"
	"github.com/google/uuid"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

const (
	statsRolloverNamespace = "freeform-pipeline-executor"
	statsMetricDelivered   = "total_events_delivered"
	statsMetricDLQ         = "total_events_dlq"
)

type statsRolloverFixture struct {
	tenantID  string
	sourceID  string
	index     string
	hourStart time.Time
	tsWin     int64
}

func newStatsRolloverFixture() statsRolloverFixture {
	tenantID := uuid.NewString()
	sourceID := uuid.NewString()
	hourStart := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	return statsRolloverFixture{
		tenantID:  tenantID,
		sourceID:  sourceID,
		index:     fmt.Sprintf("db_statistics_%s_v2_p1_y2024_d166", tenantID),
		hourStart: hourStart,
		tsWin:     hourStart.UnixMilli(),
	}
}

func deleteStatsTestIndex(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, indexName string) {
	t.Helper()
	if indexName == "" {
		return
	}
	req := opensearchapi.IndicesDeleteRequest{Index: []string{indexName}}
	resp, err := req.Do(ctx, openSearch.GetClient())
	if err != nil {
		t.Fatalf("delete stats index %s: %v", indexName, err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		raw, _ := io.ReadAll(resp.Body)
		if !bytes.Contains(raw, []byte("index_not_found_exception")) {
			t.Fatalf("delete stats index %s failed, status %d: %s", indexName, resp.StatusCode, string(raw))
		}
	}
}

func statsDocBody(fixture statsRolloverFixture, metricName, operatorID string, count float64, tsWin int64) map[string]any {
	tags := map[string]any{
		"db_tenant_id":       fixture.tenantID,
		"db_event_source_id": fixture.sourceID,
		"db_ts_win":          tsWin,
	}
	if operatorID != "" {
		tags["operator_id"] = operatorID
	}
	return map[string]any{
		"name":      metricName,
		"namespace": statsRolloverNamespace,
		"counter": map[string]any{
			"value": count,
		},
		"tags":      tags,
		"timestamp": tsWin,
	}
}

func indexStatsDocuments(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, indexName string, docs []map[string]any) {
	t.Helper()
	rows := make([]map[string]any, 0, len(docs))
	for i, doc := range docs {
		rows = append(rows, map[string]any{
			"_id":  fmt.Sprintf("stats-doc-%d", i),
			"body": doc,
		})
	}
	bulkIndexDocuments(t, ctx, openSearch, indexName, rows)
	refreshIndex(t, ctx, openSearch, indexName)
}

type statsOperatorMetric struct {
	metricName string
	operatorID string
	total      float64
}

func sumStatsByMetricAndOperator(
	t *testing.T,
	ctx context.Context,
	openSearch *pramaan.OpenSearchPramaan,
	indexName string,
) map[statsOperatorMetric]float64 {
	t.Helper()

	aggregations := []dbos.AggregationFunction{
		{Name: "total_count", Field: "counter.value", Function: "sum"},
	}
	groupBy := []string{"name.raw", "tags.operator_id.keyword"}
	// Lucene query_string requires *:* for match-all; bare "*" matches nothing.
	query := "*:*"

	totals := make(map[statsOperatorMetric]float64)
	var after map[string]any
	for {
		aggs, newAfter, err := dbos.CompositePaginatedAggregateWithNAMissing(ctx, openSearch.GetClient(), 100, indexName, query, groupBy, []string{"tags.operator_id.keyword"}, aggregations, after)
		if err != nil {
			t.Fatalf("composite aggregate on %s: %v", indexName, err)
		}
		if len(aggs) == 0 {
			break
		}
		for _, agg := range aggs {
			metricName, _ := agg.Key["name.raw"].(string)
			operatorID, _ := agg.Key["tags.operator_id.keyword"].(string)
			if operatorID == "" {
				operatorID = "N/A"
			}
			key := statsOperatorMetric{metricName: metricName, operatorID: operatorID}
			totals[key] = agg.Values["total_count"].(float64)
		}
		after = newAfter
		if newAfter == nil {
			break
		}
	}
	return totals
}

func sumStatsByMetric(
	t *testing.T,
	ctx context.Context,
	openSearch *pramaan.OpenSearchPramaan,
	indexName string,
) map[string]float64 {
	t.Helper()

	aggregations := []dbos.AggregationFunction{
		{Name: "total_count", Field: "counter.value", Function: "sum"},
	}
	groupBy := []string{"name.raw"}
	query := "*:*"

	totals := make(map[string]float64)
	var after map[string]any
	for {
		aggs, newAfter, err := dbos.CompositePaginatedAggregate(ctx, openSearch.GetClient(), 100, indexName, query, groupBy, aggregations, after)
		if err != nil {
			t.Fatalf("composite aggregate on %s: %v", indexName, err)
		}
		if len(aggs) == 0 {
			break
		}
		for _, agg := range aggs {
			metricName, _ := agg.Key["name.raw"].(string)
			totals[metricName] = agg.Values["total_count"].(float64)
		}
		after = newAfter
		if newAfter == nil {
			break
		}
	}
	return totals
}

func assertMetricTotal(t *testing.T, totals map[string]float64, metricName string, want float64) {
	t.Helper()
	got := totals[metricName]
	if got != want {
		t.Fatalf("%s total = %v, want %v (all totals: %+v)", metricName, got, want, totals)
	}
}

func assertStatsTotal(
	t *testing.T,
	totals map[statsOperatorMetric]float64,
	metricName, operatorID string,
	want float64,
) {
	t.Helper()
	got := totals[statsOperatorMetric{metricName: metricName, operatorID: operatorID}]
	if got != want {
		t.Fatalf("%s operator=%q total = %v, want %v (all totals: %+v)", metricName, operatorID, got, want, totals)
	}
}
