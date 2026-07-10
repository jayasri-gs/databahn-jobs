//go:build integration

package integrationtests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/databahn-ai/pramaan-go/pramaan"
	"github.com/google/uuid"
)

func TestInsightsAggregationCreatesSightsFromStaging(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	fixture := seedInsightsTenant(t)
	data := newInsightsAggTestData(fixture, buildMultiBucketStagingDocs(fixture))
	cleanupInsightsTestIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), data.stagingTime)
	indexStagingInsights(t, ctx, job.GetOpenSearch(t), data)

	runInsightsAggregationJob(t, ctx, tg, job, nil)
	refreshInsightsIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String())

	sightsIndex := sightsIndexName(fixture.TenantID.String())
	gotCount := countIndexDocuments(t, ctx, job.GetOpenSearch(t), sightsIndex)
	wantCount := insightsAggStagingPageDocs - insightsAggMultiBucketCount + 1
	if gotCount != wantCount {
		t.Fatalf("sights document count = %d, want %d", gotCount, wantCount)
	}

	merged := getSightDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), insightsAggMultiBucketHost, fixture.SourceID.String())
	assertSightDocument(t, merged, fixture, insightsAggMultiBucketHost, 100, 250)

	firstUniqueHost := "host-0"
	firstUnique := getSightDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), firstUniqueHost, fixture.SourceID.String())
	assertSightDocument(t, firstUnique, fixture, firstUniqueHost, 1_000_000, 1_000_100)

	lastUniqueHost := fmt.Sprintf("host-%d", insightsAggStagingPageDocs-insightsAggMultiBucketCount-1)
	lastUnique := getSightDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), lastUniqueHost, fixture.SourceID.String())
	assertSightDocument(t, lastUnique, fixture, lastUniqueHost, 1_000_000+int64(insightsAggStagingPageDocs-insightsAggMultiBucketCount-1), 1_000_100+int64(insightsAggStagingPageDocs-insightsAggMultiBucketCount-1))
}

func TestInsightsAggregationMergesIntoExistingSights(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	fixture := seedInsightsTenant(t)
	const host = "existing-sight-host"
	data := newInsightsAggTestData(fixture, buildSingleHostStagingDocs(fixture, host, 100, 900))
	cleanupInsightsTestIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), data.stagingTime)
	indexSightDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host, fixture.SourceID.String(), fixture.DataPlaneID.String(), 500, 600)
	indexStagingInsights(t, ctx, job.GetOpenSearch(t), data)

	runInsightsAggregationJob(t, ctx, tg, job, nil)
	refreshInsightsIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String())

	merged := getSightDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host, fixture.SourceID.String())
	assertInt64Field(t, merged, "min_time", 100)
	assertInt64Field(t, merged, "max_time", 900)
}

func TestInsightsAggregationDeviceAggCreatesDeviceIndex(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	fixture := seedInsightsTenant(t)
	data := newInsightsAggTestData(fixture, buildUniqueStagingDocs(fixture, insightsAggStagingPageDocs, 2_000_000))
	cleanupInsightsTestIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), data.stagingTime)
	indexStagingInsights(t, ctx, job.GetOpenSearch(t), data)

	runInsightsAggregationJob(t, ctx, tg, job, map[string]string{
		"DEVICE_AGG": "AGG",
	})
	refreshInsightsIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String())

	devicesIndex := devicesIndexName(fixture.TenantID.String())
	gotCount := countIndexDocuments(t, ctx, job.GetOpenSearch(t), devicesIndex)
	if gotCount != insightsAggStagingPageDocs {
		t.Fatalf("device document count = %d, want %d", gotCount, insightsAggStagingPageDocs)
	}

	sampleHost := "host-0"
	device := getDeviceDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), sampleHost)
	assertInt64Field(t, device, "global_min_time", 2_000_000)
	assertInt64Field(t, device, "global_max_time", 2_000_100)
	assertDeviceSourceCount(t, device, 1)
}

func TestInsightsAggregationDeviceAggMergesExistingDeviceIndex(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	fixture := seedInsightsTenant(t)
	const host = "existing-device-host"
	data := newInsightsAggTestData(fixture, buildSingleHostStagingDocs(fixture, host, 100, 900))
	cleanupInsightsTestIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), data.stagingTime)
	indexDeviceDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host, fixture.SourceID.String(), fixture.DataPlaneID.String(), 500, 600)
	indexStagingInsights(t, ctx, job.GetOpenSearch(t), data)

	runInsightsAggregationJob(t, ctx, tg, job, map[string]string{
		"DEVICE_AGG": "AGG",
	})
	refreshInsightsIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String())

	device := getDeviceDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host)
	assertInt64Field(t, device, "global_min_time", 100)
	assertInt64Field(t, device, "global_max_time", 900)
	if tz, _ := device["device_timezone"].(string); tz != "America/New_York" {
		t.Fatalf("device_timezone = %q, want preserved user value America/New_York", tz)
	}
	assertDeviceSourceCount(t, device, 1)
}

func TestInsightsAggregationDeviceAggAddsSourceToExistingDevice(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	fixture := seedInsightsTenant(t)
	const host = "existing-device-add-source-host"
	alternateSourceID := uuid.NewString()
	data := newInsightsAggTestData(fixture, buildSingleHostStagingDocsForSource(fixture, host, alternateSourceID, 200, 800))
	cleanupInsightsTestIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), data.stagingTime)
	indexDeviceDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host, fixture.SourceID.String(), fixture.DataPlaneID.String(), 500, 600)
	indexStagingInsights(t, ctx, job.GetOpenSearch(t), data)

	runInsightsAggregationJob(t, ctx, tg, job, map[string]string{
		"DEVICE_AGG": "AGG",
	})
	refreshInsightsIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String())

	device := getDeviceDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host)
	assertInt64Field(t, device, "global_min_time", 200)
	assertInt64Field(t, device, "global_max_time", 800)
	assertDeviceSourceCount(t, device, 2)
	assertDeviceSource(t, device, fixture.SourceID.String(), 500, 600)
	assertDeviceSource(t, device, alternateSourceID, 200, 800)
}

func TestInsightsAggregationDeviceAggMergesSourceOnExistingDevice(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	fixture := seedInsightsTenant(t)
	const host = "existing-device-merge-source-host"
	data := newInsightsAggTestData(fixture, buildSingleHostStagingDocs(fixture, host, 100, 900))
	cleanupInsightsTestIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), data.stagingTime)
	indexDeviceDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host, fixture.SourceID.String(), fixture.DataPlaneID.String(), 500, 600)
	indexStagingInsights(t, ctx, job.GetOpenSearch(t), data)

	runInsightsAggregationJob(t, ctx, tg, job, map[string]string{
		"DEVICE_AGG": "AGG",
	})
	refreshInsightsIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String())

	device := getDeviceDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host)
	assertInt64Field(t, device, "global_min_time", 100)
	assertInt64Field(t, device, "global_max_time", 900)
	assertDeviceSourceCount(t, device, 1)
	assertDeviceSource(t, device, fixture.SourceID.String(), 100, 900)
}

func TestInsightsAggregationDeviceBackfillCreatesDeviceIndex(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	fixture := seedInsightsTenant(t)
	cleanupInsightsTestIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), time.Now().UTC().Add(-48*time.Hour))
	const backfillDocCount = 20
	for i := 0; i < backfillDocCount; i++ {
		host := fmt.Sprintf("backfill-host-%d", i)
		minTime := int64(3_000_000 + i)
		maxTime := minTime + 50
		indexSightDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), host, fixture.SourceID.String(), fixture.DataPlaneID.String(), minTime, maxTime)
	}

	runInsightsAggregationJob(t, ctx, tg, job, map[string]string{
		"DEVICE_AGG": "BACKFILL",
	})
	refreshInsightsIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String())

	devicesIndex := devicesIndexName(fixture.TenantID.String())
	gotCount := countIndexDocuments(t, ctx, job.GetOpenSearch(t), devicesIndex)
	if gotCount != backfillDocCount {
		t.Fatalf("device document count after backfill = %d, want %d", gotCount, backfillDocCount)
	}

	sample := getDeviceDocument(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), "backfill-host-0")
	assertInt64Field(t, sample, "global_min_time", 3_000_000)
	assertInt64Field(t, sample, "global_max_time", 3_000_050)
	assertDeviceSourceCount(t, sample, 1)
}
