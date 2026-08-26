//go:build integration

package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
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

	fieldOperatorID   = "tags.operator_id.keyword"
	fieldTargetNodeID = "tags.target_node_id.keyword"
	fieldRouteKey     = "tags.route_key.keyword"
	fieldRuleID       = "tags.rule_id.keyword"
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
	return statsEdgeDocBody(fixture, metricName, statsEdgeDocOpts{operatorID: operatorID}, count, tsWin)
}

type statsEdgeDocOpts struct {
	operatorID   string
	targetNodeID string
	routeKey     string
	ruleID       string
}

func statsEdgeDocBody(
	fixture statsRolloverFixture,
	metricName string,
	opts statsEdgeDocOpts,
	count float64,
	tsWin int64,
) map[string]any {
	tags := map[string]any{
		"db_tenant_id":       fixture.tenantID,
		"db_event_source_id": fixture.sourceID,
		"db_ts_win":          tsWin,
	}
	if opts.operatorID != "" {
		tags["operator_id"] = opts.operatorID
	}
	if opts.targetNodeID != "" {
		tags["target_node_id"] = opts.targetNodeID
	}
	if opts.routeKey != "" {
		tags["route_key"] = opts.routeKey
	}
	if opts.ruleID != "" {
		tags["rule_id"] = opts.ruleID
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

type statsEdgeMetric struct {
	metricName   string
	operatorID   string
	targetNodeID string
	routeKey     string
	ruleID       string
}

var statsEdgeGroupBy = []string{
	"name.raw",
	fieldOperatorID,
	fieldTargetNodeID,
	fieldRouteKey,
	fieldRuleID,
}

var statsEdgeMissingAsNA = []string{
	fieldOperatorID,
	fieldTargetNodeID,
	fieldRouteKey,
	fieldRuleID,
}

func normalizeStatsAggTag(value string) string {
	if value == "" {
		return "N/A"
	}
	return value
}

func sumStatsByEdgeMetric(
	t *testing.T,
	ctx context.Context,
	openSearch *pramaan.OpenSearchPramaan,
	indexName string,
) map[statsEdgeMetric]float64 {
	t.Helper()

	aggregations := []dbos.AggregationFunction{
		{Name: "total_count", Field: "counter.value", Function: "sum"},
	}
	query := "*:*"

	totals := make(map[statsEdgeMetric]float64)
	var after map[string]any
	for {
		aggs, newAfter, err := dbos.CompositePaginatedAggregateWithNAMissing(
			ctx,
			openSearch.GetClient(),
			100,
			indexName,
			query,
			statsEdgeGroupBy,
			statsEdgeMissingAsNA,
			aggregations,
			after,
		)
		if err != nil {
			t.Fatalf("composite edge aggregate on %s: %v", indexName, err)
		}
		if len(aggs) == 0 {
			break
		}
		for _, agg := range aggs {
			key := statsEdgeMetric{
				metricName:   agg.Key["name.raw"].(string),
				operatorID:   normalizeStatsAggTag(aggKeyString(agg.Key, fieldOperatorID)),
				targetNodeID: normalizeStatsAggTag(aggKeyString(agg.Key, fieldTargetNodeID)),
				routeKey:     normalizeStatsAggTag(aggKeyString(agg.Key, fieldRouteKey)),
				ruleID:       normalizeStatsAggTag(aggKeyString(agg.Key, fieldRuleID)),
			}
			totals[key] = agg.Values["total_count"].(float64)
		}
		after = newAfter
		if newAfter == nil {
			break
		}
	}
	return totals
}

func assertStatsEdgeTotal(
	t *testing.T,
	totals map[statsEdgeMetric]float64,
	metricName, operatorID, targetNodeID, routeKey, ruleID string,
	want float64,
) {
	t.Helper()
	key := statsEdgeMetric{
		metricName:   metricName,
		operatorID:   normalizeStatsAggTag(operatorID),
		targetNodeID: normalizeStatsAggTag(targetNodeID),
		routeKey:     normalizeStatsAggTag(routeKey),
		ruleID:       normalizeStatsAggTag(ruleID),
	}
	got := totals[key]
	if got != want {
		t.Fatalf("edge %s %s->%s %s rule=%q total = %v, want %v (all totals: %+v)",
			metricName, operatorID, targetNodeID, routeKey, ruleID, got, want, totals)
	}
}

func aggKeyString(key map[string]any, groupField string) string {
	if v, ok := key[groupField]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func assertRolledStatsDocTag(
	t *testing.T,
	ctx context.Context,
	openSearch *pramaan.OpenSearchPramaan,
	indexName string,
	operatorID, targetNodeID, routeKey, wantRuleID string,
) {
	t.Helper()

	query := map[string]any{
		"size": 1,
		"query": map[string]any{
			"bool": map[string]any{
				"must": []map[string]any{
					{"term": map[string]any{fieldOperatorID: operatorID}},
					{"term": map[string]any{fieldTargetNodeID: targetNodeID}},
					{"term": map[string]any{fieldRouteKey: routeKey}},
				},
			},
		},
	}
	result, err := openSearch.SearchDocuments(ctx, indexName, query)
	if err != nil {
		t.Fatalf("search rolled stats doc: %v", err)
	}
	hitsRoot, ok := result["hits"].(map[string]any)
	if !ok {
		t.Fatalf("search hits missing: %+v", result)
	}
	hits, ok := hitsRoot["hits"].([]any)
	if !ok || len(hits) == 0 {
		t.Fatalf("no rolled doc found for operator=%s target=%s route=%s", operatorID, targetNodeID, routeKey)
	}
	first, ok := hits[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected hit shape: %T", hits[0])
	}
	source, ok := first["_source"].(map[string]any)
	if !ok {
		t.Fatalf("missing _source on hit")
	}
	tags, ok := source["tags"].(map[string]any)
	if !ok {
		t.Fatalf("missing tags on rolled doc: %+v", source)
	}
	if got, _ := tags["rule_id"].(string); got != wantRuleID {
		t.Fatalf("rolled doc rule_id = %q, want %q (tags=%+v)", got, wantRuleID, tags)
	}
	if got, _ := tags["target_node_id"].(string); got != targetNodeID {
		t.Fatalf("rolled doc target_node_id = %q, want %q", got, targetNodeID)
	}
	if got, _ := tags["route_key"].(string); got != routeKey {
		t.Fatalf("rolled doc route_key = %q, want %q", got, routeKey)
	}
}

func assertStatsIndexMapsEdgeTagKeyword(
	t *testing.T,
	ctx context.Context,
	openSearch *pramaan.OpenSearchPramaan,
	indexName, tagField string,
) {
	t.Helper()

	req := opensearchapi.IndicesGetMappingRequest{Index: []string{indexName}}
	resp, err := req.Do(ctx, openSearch.GetClient())
	if err != nil {
		t.Fatalf("get mapping for %s: %v", indexName, err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("get mapping failed, status %d: %s", resp.StatusCode, string(raw))
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode mapping: %v", err)
	}
	indexMapping, ok := body[indexName].(map[string]any)
	if !ok {
		for _, v := range body {
			indexMapping, ok = v.(map[string]any)
			if ok {
				break
			}
		}
	}
	if !ok {
		t.Fatalf("index mapping missing for %s: %+v", indexName, body)
	}
	mappings, ok := indexMapping["mappings"].(map[string]any)
	if !ok {
		t.Fatalf("mappings missing: %+v", indexMapping)
	}
	properties, ok := mappings["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties missing: %+v", mappings)
	}
	tags, ok := properties["tags"].(map[string]any)
	if !ok {
		t.Fatalf("tags missing: %+v", properties)
	}
	tagProps, ok := tags["properties"].(map[string]any)
	if !ok {
		t.Fatalf("tags.properties missing: %+v", tags)
	}
	fieldMapping, ok := tagProps[tagField].(map[string]any)
	if !ok {
		t.Fatalf("tags.%s mapping missing: %+v", tagField, tagProps)
	}
	fields, ok := fieldMapping["fields"].(map[string]any)
	if !ok {
		t.Fatalf("tags.%s.fields missing", tagField)
	}
	keyword, ok := fields["keyword"].(map[string]any)
	if !ok || keyword["type"] != "keyword" {
		t.Fatalf("tags.%s.fields.keyword missing or wrong type: %+v", tagField, fields)
	}
}
