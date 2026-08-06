//go:build integration

package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
	"github.com/databahn-ai/databahn-jobs/internal/insights"
	"github.com/databahn-ai/pramaan-go/pramaan"
	"github.com/google/uuid"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

const (
	insightsAggJobName          = "insights_aggregation"
	insightsAggReadBatch        = "10"
	insightsAggJobTimeout       = 3 * time.Minute
	insightsAggStagingPageDocs  = 45
	insightsAggMultiBucketHost  = "agg-merge-host"
	insightsAggMultiBucketCount = 3
)

type stagingInsightsDoc struct {
	ID          string
	Key1        string
	Key2        string
	Key3        string
	Key4        string
	SourceID    string
	TenantID    string
	DataPlaneID string
	MinTime     int64
	MaxTime     int64
	Count       int64
	Timestamp   int64
}

type insightsAggTestData struct {
	fixture     *fixtures.InsightsFixture
	stagingTime time.Time
	stagingDocs []stagingInsightsDoc
}

func newInsightsAggTestData(fixture *fixtures.InsightsFixture, stagingDocs []stagingInsightsDoc) insightsAggTestData {
	return insightsAggTestData{
		fixture:     fixture,
		stagingTime: time.Now().UTC().Add(-48 * time.Hour),
		stagingDocs: stagingDocs,
	}
}

func seedInsightsTenant(t *testing.T) *fixtures.InsightsFixture {
	t.Helper()
	fixture, err := fixtures.SeedInsightsFixture(context.Background(), GormDB())
	if err != nil {
		t.Fatalf("seed insights fixture: %v", err)
	}
	return fixture
}

func stagingInsightsIndexName(tenantID string, ts time.Time) string {
	return fmt.Sprintf("db_staging_insights_v1_sourcehostname_%04d_%02d_%02d_%02d_%s",
		ts.Year(), int(ts.Month()), ts.Day(), ts.Hour(), tenantID)
}

func sightsIndexName(tenantID string) string {
	return insights.SightIndexName(tenantID)
}

func devicesIndexName(tenantID string) string {
	return insights.DeviceIndexName(tenantID)
}

func sightDocumentID(key1, sourceID string) string {
	return insights.InsightId(key1, "", "", "", "", sourceID)
}

func deviceDocumentID(tenantID, key1 string) string {
	return insights.DeviceId(tenantID, key1)
}

func buildUniqueStagingDocs(fixture *fixtures.InsightsFixture, count int, minTimeBase int64) []stagingInsightsDoc {
	docs := make([]stagingInsightsDoc, 0, count)
	now := time.Now().UTC().UnixMilli()
	for i := 0; i < count; i++ {
		docs = append(docs, stagingInsightsDoc{
			ID:          uuid.NewString(),
			Key1:        fmt.Sprintf("host-%d", i),
			SourceID:    fixture.SourceID.String(),
			TenantID:    fixture.TenantID.String(),
			DataPlaneID: fixture.DataPlaneID.String(),
			MinTime:     minTimeBase + int64(i),
			MaxTime:     minTimeBase + int64(i) + 100,
			Count:       1,
			Timestamp:   now,
		})
	}
	return docs
}

func buildMultiBucketStagingDocs(fixture *fixtures.InsightsFixture) []stagingInsightsDoc {
	uniqueDocs := buildUniqueStagingDocs(fixture, insightsAggStagingPageDocs-insightsAggMultiBucketCount, 1_000_000)
	now := time.Now().UTC().UnixMilli()
	multi := []stagingInsightsDoc{
		{
			ID: uuid.NewString(), Key1: insightsAggMultiBucketHost, SourceID: fixture.SourceID.String(),
			TenantID: fixture.TenantID.String(), DataPlaneID: fixture.DataPlaneID.String(),
			MinTime: 100, MaxTime: 200, Count: 1, Timestamp: now,
		},
		{
			ID: uuid.NewString(), Key1: insightsAggMultiBucketHost, SourceID: fixture.SourceID.String(),
			TenantID: fixture.TenantID.String(), DataPlaneID: fixture.DataPlaneID.String(),
			MinTime: 150, MaxTime: 180, Count: 2, Timestamp: now,
		},
		{
			ID: uuid.NewString(), Key1: insightsAggMultiBucketHost, SourceID: fixture.SourceID.String(),
			TenantID: fixture.TenantID.String(), DataPlaneID: fixture.DataPlaneID.String(),
			MinTime: 120, MaxTime: 250, Count: 3, Timestamp: now,
		},
	}
	return append(uniqueDocs, multi...)
}

func buildSingleHostStagingDocs(fixture *fixtures.InsightsFixture, key1 string, minTime, maxTime int64) []stagingInsightsDoc {
	return buildSingleHostStagingDocsForSource(fixture, key1, fixture.SourceID.String(), minTime, maxTime)
}

func buildSingleHostStagingDocsForSource(fixture *fixtures.InsightsFixture, key1, sourceID string, minTime, maxTime int64) []stagingInsightsDoc {
	now := time.Now().UTC().UnixMilli()
	return []stagingInsightsDoc{
		{
			ID: uuid.NewString(), Key1: key1, SourceID: sourceID,
			TenantID: fixture.TenantID.String(), DataPlaneID: fixture.DataPlaneID.String(),
			MinTime: minTime, MaxTime: maxTime, Count: 1, Timestamp: now,
		},
	}
}

func indexStagingInsights(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, data insightsAggTestData) string {
	t.Helper()
	indexName := stagingInsightsIndexName(data.fixture.TenantID.String(), data.stagingTime)
	bulkIndexDocuments(t, ctx, openSearch, indexName, stagingDocsToBulk(data.stagingDocs))
	refreshIndex(t, ctx, openSearch, indexName)
	return indexName
}

func stagingDocsToBulk(docs []stagingInsightsDoc) []map[string]any {
	rows := make([]map[string]any, 0, len(docs))
	for _, doc := range docs {
		rows = append(rows, map[string]any{
			"_id":  doc.ID,
			"body": stagingDocBody(doc),
		})
	}
	return rows
}

func stagingDocBody(doc stagingInsightsDoc) map[string]any {
	return map[string]any{
		"id":            doc.ID,
		"key1":          doc.Key1,
		"key2":          doc.Key2,
		"key3":          doc.Key3,
		"key4":          doc.Key4,
		"key5":          "",
		"source_id":     doc.SourceID,
		"data_plane_id": doc.DataPlaneID,
		"tenantId":      doc.TenantID,
		"type":          insights.APP_TYPE_SOURCEHOSTNAME,
		"count":         doc.Count,
		"min_time":      doc.MinTime,
		"max_time":      doc.MaxTime,
		"timestamp":     doc.Timestamp,
	}
}

func indexSightDocument(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, tenantID, key1, sourceID, dataPlaneID string, minTime, maxTime int64) {
	t.Helper()
	now := time.Now().UTC().UnixMilli()
	doc := map[string]any{
		"id":            sightDocumentID(key1, sourceID),
		"key1":          key1,
		"key2":          "",
		"key3":          "",
		"tenant_id":     tenantID,
		"source_id":     sourceID,
		"data_plane_id": dataPlaneID,
		"min_time":      minTime,
		"max_time":      maxTime,
		"timestamp":     now,
		"updated_at":    now,
		"reputation":    insights.REPUTATION_NORMAL,
	}
	index := sightsIndexName(tenantID)
	if err := openSearch.IndexDocument(ctx, index, sightDocumentID(key1, sourceID), doc); err != nil {
		t.Fatalf("index sight document: %v", err)
	}
	refreshIndex(t, ctx, openSearch, index)
}

func indexDeviceDocument(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, tenantID, key1, sourceID, dataPlaneID string, minTime, maxTime int64) {
	t.Helper()
	now := time.Now().UTC().UnixMilli()
	docID := deviceDocumentID(tenantID, key1)
	doc := map[string]any{
		"id":                     docID,
		"key1":                   key1,
		"tenant_id":              tenantID,
		"data_plane_id":          dataPlaneID,
		"global_min_time":        minTime,
		"global_max_time":        maxTime,
		"timestamp":              now,
		"updated_at":             now,
		"device_timezone":        "America/New_York",
		"timezone_update_reason": insights.TimezoneUpdateReasonManual,
		"sources": []map[string]any{
			{
				"source_id": sourceID,
				"min_time":  minTime,
				"max_time":  maxTime,
			},
		},
	}
	index := devicesIndexName(tenantID)
	if err := openSearch.IndexDocument(ctx, index, docID, doc); err != nil {
		t.Fatalf("index device document: %v", err)
	}
	refreshIndex(t, ctx, openSearch, index)
}

func indexManualTimezoneDeviceDocument(
	t *testing.T,
	ctx context.Context,
	openSearch *pramaan.OpenSearchPramaan,
	tenantID, key1, sourceID, dataPlaneID, deviceTimezone, timezoneUpdatedBy string,
	timezoneUpdatedAt, minTime, maxTime int64,
) {
	t.Helper()
	now := time.Now().UTC().UnixMilli()
	docID := deviceDocumentID(tenantID, key1)
	doc := map[string]any{
		"id":                     docID,
		"key1":                   key1,
		"tenant_id":              tenantID,
		"data_plane_id":          dataPlaneID,
		"global_min_time":        minTime,
		"global_max_time":        maxTime,
		"timestamp":              now,
		"updated_at":             now,
		"device_timezone":        deviceTimezone,
		"timezone_updated_at":    timezoneUpdatedAt,
		"timezone_updated_by":    timezoneUpdatedBy,
		"timezone_update_reason": insights.TimezoneUpdateReasonManual,
		"sources": []map[string]any{
			{
				"source_id": sourceID,
				"min_time":  minTime,
				"max_time":  maxTime,
			},
		},
	}
	index := devicesIndexName(tenantID)
	if err := openSearch.IndexDocument(ctx, index, docID, doc); err != nil {
		t.Fatalf("index manual timezone device document: %v", err)
	}
	refreshIndex(t, ctx, openSearch, index)
}

func assertAgentDetectedDeviceTimezone(t *testing.T, device map[string]any, wantTimezone string, before time.Time) {
	t.Helper()
	assertStringField(t, device, "device_timezone", wantTimezone)
	assertStringField(t, device, "timezone_updated_by", insights.AgentTimezoneUpdatedByUUID)
	assertStringField(t, device, "timezone_update_reason", insights.TimezoneUpdateReasonAgentSetting)

	updatedAt, ok := device["timezone_updated_at"].(float64)
	if !ok || updatedAt <= 0 {
		t.Fatalf("timezone_updated_at = %#v, want positive timestamp", device["timezone_updated_at"])
	}
	if int64(updatedAt) < before.UnixMilli() {
		t.Fatalf("timezone_updated_at %d is before test start %d", int64(updatedAt), before.UnixMilli())
	}
}

func assertDeviceTimezoneUnset(t *testing.T, device map[string]any) {
	t.Helper()
	if tz, ok := device["device_timezone"].(string); ok && tz != "" {
		t.Fatalf("device_timezone = %q, want unset", tz)
	}
	if reason, ok := device["timezone_update_reason"].(string); ok && reason == insights.TimezoneUpdateReasonAgentSetting {
		t.Fatalf("timezone_update_reason = %q, want not agent_setting", reason)
		t.Fatalf("timezone_update_reason = %q, want not agent_setting", reason)
	}
	if updatedBy, ok := device["timezone_updated_by"].(string); ok && updatedBy == insights.AgentTimezoneUpdatedByUUID {
		t.Fatalf("timezone_updated_by = agent uuid, want unset")
	}
}

func bulkIndexDocuments(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, indexName string, docs []map[string]any) {
	t.Helper()
	if len(docs) == 0 {
		return
	}

	var buff bytes.Buffer
	for _, doc := range docs {
		id, _ := doc["_id"].(string)
		body, _ := doc["body"].(map[string]any)
		meta, err := json.Marshal(map[string]any{
			"index": map[string]any{"_index": indexName, "_id": id},
		})
		if err != nil {
			t.Fatalf("marshal bulk meta: %v", err)
		}
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal bulk body: %v", err)
		}
		buff.Write(meta)
		buff.WriteByte('\n')
		buff.Write(payload)
		buff.WriteByte('\n')
	}

	req := opensearchapi.BulkRequest{Body: &buff, Refresh: "false"}
	resp, err := req.Do(ctx, openSearch.GetClient())
	if err != nil {
		t.Fatalf("bulk index documents: %v", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("bulk index failed, status %d: %s", resp.StatusCode, string(body))
	}

	var bulkResp struct {
		Errors bool `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&bulkResp); err != nil {
		t.Fatalf("decode bulk response: %v", err)
	}
	if bulkResp.Errors {
		t.Fatalf("bulk index returned errors for index %s", indexName)
	}
}

func refreshIndex(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, indexName string) {
	t.Helper()
	req := opensearchapi.IndicesRefreshRequest{Index: []string{indexName}}
	resp, err := req.Do(ctx, openSearch.GetClient())
	if err != nil {
		t.Fatalf("refresh index %s: %v", indexName, err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		if resp.StatusCode == http.StatusNotFound {
			return
		}
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("refresh index %s failed, status %d: %s", indexName, resp.StatusCode, string(body))
	}
}

func countIndexDocuments(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, indexName string) int {
	t.Helper()
	result, err := openSearch.SearchDocuments(ctx, indexName, map[string]any{
		"size":             0,
		"track_total_hits": true,
		"query":            map[string]any{"match_all": map[string]any{}},
	})
	if err != nil {
		t.Fatalf("count documents in %s: %v", indexName, err)
	}

	hits, ok := result["hits"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected search response for %s: %#v", indexName, result)
	}
	total, ok := hits["total"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected total in search response for %s: %#v", indexName, hits)
	}
	value, ok := total["value"].(float64)
	if !ok {
		t.Fatalf("unexpected total value for %s: %#v", indexName, total)
	}
	return int(value)
}

func getSightDocument(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, tenantID, key1, sourceID string) map[string]any {
	t.Helper()
	raw, err := openSearch.GetDocument(ctx, sightsIndexName(tenantID), sightDocumentID(key1, sourceID))
	if err != nil {
		t.Fatalf("get sight document: %v", err)
	}
	source, ok := raw["_source"].(map[string]any)
	if !ok {
		t.Fatalf("sight document missing _source: %#v", raw)
	}
	return source
}

func getDeviceDocument(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, tenantID, key1 string) map[string]any {
	t.Helper()
	raw, err := openSearch.GetDocument(ctx, devicesIndexName(tenantID), deviceDocumentID(tenantID, key1))
	if err != nil {
		t.Fatalf("get device document: %v", err)
	}
	source, ok := raw["_source"].(map[string]any)
	if !ok {
		t.Fatalf("device document missing _source: %#v", raw)
	}
	return source
}

func assertInt64Field(t *testing.T, doc map[string]any, field string, want int64) {
	t.Helper()
	got, ok := doc[field].(float64)
	if !ok {
		t.Fatalf("field %q missing or wrong type in %#v", field, doc)
	}
	if int64(got) != want {
		t.Fatalf("field %q = %d, want %d", field, int64(got), want)
	}
}

func assertStringField(t *testing.T, doc map[string]any, field, want string) {
	t.Helper()
	got, ok := doc[field].(string)
	if !ok {
		t.Fatalf("field %q missing or wrong type in %#v", field, doc)
	}
	if got != want {
		t.Fatalf("field %q = %q, want %q", field, got, want)
	}
}

func assertSightDocument(
	t *testing.T,
	doc map[string]any,
	fixture *fixtures.InsightsFixture,
	host string,
	wantMinTime, wantMaxTime int64,
) {
	t.Helper()
	assertStringField(t, doc, "key1", host)
	assertStringField(t, doc, "tenant_id", fixture.TenantID.String())
	assertStringField(t, doc, "source_id", fixture.SourceID.String())
	assertInt64Field(t, doc, "min_time", wantMinTime)
	assertInt64Field(t, doc, "max_time", wantMaxTime)
}

func assertDeviceSourceCount(t *testing.T, doc map[string]any, want int) {
	t.Helper()
	sources, ok := doc["sources"].([]any)
	if !ok {
		t.Fatalf("device sources missing or wrong type: %#v", doc)
	}
	if len(sources) != want {
		t.Fatalf("device source count = %d, want %d", len(sources), want)
	}
}

func assertDeviceSource(t *testing.T, doc map[string]any, sourceID string, wantMinTime, wantMaxTime int64) {
	t.Helper()
	sources, ok := doc["sources"].([]any)
	if !ok {
		t.Fatalf("device sources missing or wrong type: %#v", doc)
	}
	for _, entry := range sources {
		src, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("device source entry has wrong type: %#v", entry)
		}
		id, ok := src["source_id"].(string)
		if !ok || id != sourceID {
			continue
		}
		assertInt64Field(t, src, "min_time", wantMinTime)
		assertInt64Field(t, src, "max_time", wantMaxTime)
		return
	}
	t.Fatalf("source_id %q not found in %#v", sourceID, sources)
}

func runInsightsAggregationJob(t *testing.T, ctx context.Context, tg pramaan.TestLogger, job *pramaan.JobPramaan, extraEnv map[string]string) {
	t.Helper()
	env := map[string]string{
		"INSIGHTS_AGG_SKIP_INDEX_TIME_CHECK":     "true",
		"INSIGHTS_READ_BATCH":                    insightsAggReadBatch,
		"INSIGHTS_TEST_SKIP_OBJECT_STORE_UPLOAD": "true",
	}
	for key, value := range extraEnv {
		env[key] = value
	}

	job.Run(ctx, tg, pramaan.JobRunOptions{
		Cmd:     []string{"-job", insightsAggJobName},
		Env:     env,
		Timeout: insightsAggJobTimeout,
	})
}

func refreshInsightsIndices(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, tenantID string) {
	t.Helper()
	for _, index := range []string{sightsIndexName(tenantID), devicesIndexName(tenantID)} {
		refreshIndex(t, ctx, openSearch, index)
	}
}

func cleanupInsightsTestIndices(t *testing.T, ctx context.Context, openSearch *pramaan.OpenSearchPramaan, tenantID string, stagingTime time.Time) {
	t.Helper()
	indices := []string{
		stagingInsightsIndexName(tenantID, stagingTime),
		sightsIndexName(tenantID),
		devicesIndexName(tenantID),
	}
	for _, index := range indices {
		_ = openSearch.DeleteIndex(ctx, index)
	}
}
