//go:build integration

package integrationtests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
	"github.com/databahn-ai/pramaan-go/pramaan"
	"github.com/google/uuid"
)

func insightsFixtureFromImport(fixture *fixtures.ImportRequestFixture) *fixtures.InsightsFixture {
	return &fixtures.InsightsFixture{
		TenantID:    fixture.TenantID,
		SourceID:    fixture.SourceID,
		DataPlaneID: fixture.DataPlaneID,
	}
}

func buildMultiHostStagingDocs(
	fixture *fixtures.InsightsFixture,
	hosts []string,
	minTime, maxTime int64,
) []stagingInsightsDoc {
	docs := make([]stagingInsightsDoc, 0, len(hosts))
	for _, host := range hosts {
		docs = append(docs, buildSingleHostStagingDocs(fixture, host, minTime, maxTime)...)
	}
	return docs
}

func seedDevicesWithAggregation(
	t *testing.T,
	ctx context.Context,
	tg pramaan.TestLogger,
	job *pramaan.JobPramaan,
	fixture *fixtures.ImportRequestFixture,
	hosts []string,
) {
	t.Helper()
	if len(hosts) == 0 {
		t.Fatal("seedDevicesWithAggregation requires at least one host")
	}
	insightsFixture := insightsFixtureFromImport(fixture)
	stagingTime := time.Now().UTC().Add(-48 * time.Hour)
	data := newInsightsAggTestData(
		insightsFixture,
		buildMultiHostStagingDocs(insightsFixture, hosts, 100, 200),
	)
	data.stagingTime = stagingTime
	cleanupInsightsTestIndices(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), stagingTime)
	indexStagingInsights(t, ctx, job.GetOpenSearch(t), data)
	runInsightsAggregationJob(t, ctx, tg, job, map[string]string{
		"DEVICE_AGG": "AGG",
	})
	refreshIndex(t, ctx, job.GetOpenSearch(t), devicesIndexName(fixture.TenantID.String()))
}

func seedDeviceWithAggregation(
	t *testing.T,
	ctx context.Context,
	tg pramaan.TestLogger,
	job *pramaan.JobPramaan,
	fixture *fixtures.ImportRequestFixture,
	host string,
) {
	t.Helper()
	seedDevicesWithAggregation(t, ctx, tg, job, fixture, []string{host})
}

func TestImportRequestProcessorUpdatesMatchingDeviceTimezone(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()
	openSearch := job.GetOpenSearch(t)

	fixture := seedImportRequestFixture(t)
	const host = "timezone-host-1"

	importRequestID := uuid.New()
	filePath := fixtures.BuildImportFilePath(fixture.TenantID, importRequestID, "devices.csv")
	t.Cleanup(func() {
		cleanupImportRequestTest(t, ctx, openSearch, fixture.TenantID.String(), importRequestID)
	})

	seedDeviceWithAggregation(t, ctx, tg, job, fixture, host)

	csv := buildDeviceTimezoneCSV([]deviceTimezoneCSVRow{
		{Hostname: host, Timezone: "Europe/London"},
	})
	uploadArtifactsObject(t, ctx, job.GetCloud(t), filePath, csv)

	seed, err := fixtures.SeedDeviceTimezoneImportRequest(
		ctx, GormDB(), fixture,
		importRequestID,
		fmt.Sprintf("import-success-%s", uuid.NewString()[:8]),
		filePath,
		"devices.csv",
		true,
		fixtures.ImportStatusRequested,
	)
	if err != nil {
		t.Fatalf("seed import_request: %v", err)
	}
	if seed.ID != importRequestID {
		t.Fatalf("seed id mismatch: got %s want %s", seed.ID, importRequestID)
	}

	before := time.Now().UTC().Add(-1 * time.Minute)
	runImportRequestProcessorJob(t, ctx, tg, job)
	refreshIndex(t, ctx, openSearch, devicesIndexName(fixture.TenantID.String()))

	row := getImportRequestRow(t, ctx, importRequestID)
	if row.Status != "COMPLETED" {
		t.Fatalf("import_request status = %q, want COMPLETED (error=%q)", row.Status, row.ErrorMessage)
	}

	stats := parseImportRequestStats(t, row.Stats)
	if stats.TotalRows != 1 || stats.ProcessedRows != 1 || stats.FailedRows != 0 {
		t.Fatalf("stats = %+v, want total=1 processed=1 failed=0", stats)
	}

	device := getDeviceDocument(t, ctx, openSearch, fixture.TenantID.String(), host)
	assertStringField(t, device, "device_timezone", "Europe/London")
	assertStringField(t, device, "timezone_updated_by", fixture.ActorID.String())

	updatedAt := int64(device["timezone_updated_at"].(float64))
	if updatedAt < before.UnixMilli() {
		t.Fatalf("timezone_updated_at %d is before test start %d", updatedAt, before.UnixMilli())
	}
	assertInt64Field(t, device, "updated_at", updatedAt)
}

func TestImportRequestProcessorFailsWhenImportFileMissing(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()

	fixture := seedImportRequestFixture(t)
	importRequestID := uuid.New()
	filePath := fixtures.BuildImportFilePath(fixture.TenantID, importRequestID, "devices.csv")
	t.Cleanup(func() {
		cleanupImportRequestTest(t, ctx, job.GetOpenSearch(t), fixture.TenantID.String(), importRequestID)
	})

	seed, err := fixtures.SeedDeviceTimezoneImportRequest(
		ctx, GormDB(), fixture,
		importRequestID,
		fmt.Sprintf("import-missing-file-%s", uuid.NewString()[:8]),
		filePath,
		"devices.csv",
		true,
		fixtures.ImportStatusRequested,
	)
	if err != nil {
		t.Fatalf("seed import_request: %v", err)
	}
	if seed.ID != importRequestID {
		t.Fatalf("seed id mismatch: got %s want %s", seed.ID, importRequestID)
	}

	runImportRequestProcessorJob(t, ctx, tg, job)

	row := getImportRequestRow(t, ctx, importRequestID)
	if row.Status != "FAILED" {
		t.Fatalf("import_request status = %q, want FAILED", row.Status)
	}
	if !strings.Contains(row.ErrorMessage, "failed to read import file from object store") {
		t.Fatalf("error_message = %q, want object store read failure", row.ErrorMessage)
	}
	if row.Retries != 0 {
		t.Fatalf("retries = %d, want 0", row.Retries)
	}
}

func TestImportRequestProcessorResumesInterruptedProcessing(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()
	openSearch := job.GetOpenSearch(t)

	fixture := seedImportRequestFixture(t)
	const host = "resume-host-1"

	importRequestID := uuid.New()
	filePath := fixtures.BuildImportFilePath(fixture.TenantID, importRequestID, "devices.csv")
	t.Cleanup(func() {
		cleanupImportRequestTest(t, ctx, openSearch, fixture.TenantID.String(), importRequestID)
	})

	seedDeviceWithAggregation(t, ctx, tg, job, fixture, host)

	csv := buildDeviceTimezoneCSV([]deviceTimezoneCSVRow{
		{Hostname: host, Timezone: "Australia/Sydney"},
	})
	uploadArtifactsObject(t, ctx, job.GetCloud(t), filePath, csv)

	seed, err := fixtures.SeedDeviceTimezoneImportRequest(
		ctx, GormDB(), fixture,
		importRequestID,
		fmt.Sprintf("import-resume-%s", uuid.NewString()[:8]),
		filePath,
		"devices.csv",
		true,
		fixtures.ImportStatusProcessing,
	)
	if err != nil {
		t.Fatalf("seed import_request: %v", err)
	}
	if seed.ID != importRequestID {
		t.Fatalf("seed id mismatch: got %s want %s", seed.ID, importRequestID)
	}

	runImportRequestProcessorJob(t, ctx, tg, job)
	refreshIndex(t, ctx, openSearch, devicesIndexName(fixture.TenantID.String()))

	row := getImportRequestRow(t, ctx, importRequestID)
	if row.Status != "COMPLETED" {
		t.Fatalf("import_request status = %q, want COMPLETED (error=%q)", row.Status, row.ErrorMessage)
	}

	device := getDeviceDocument(t, ctx, openSearch, fixture.TenantID.String(), host)
	assertStringField(t, device, "device_timezone", "Australia/Sydney")
}

func TestImportRequestProcessorOnlyUpdatesExistingDevices(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()
	openSearch := job.GetOpenSearch(t)

	fixture := seedImportRequestFixture(t)
	const matchedHost = "matched-host"
	const untouchedHost = "untouched-host"
	const missingHost = "missing-host"

	missingDeviceID := deviceIDForTenant(fixture.TenantID, missingHost)

	importRequestID := uuid.New()
	filePath := fixtures.BuildImportFilePath(fixture.TenantID, importRequestID, "devices.csv")
	t.Cleanup(func() {
		cleanupImportRequestTest(t, ctx, openSearch, fixture.TenantID.String(), importRequestID)
	})

	seedDevicesWithAggregation(t, ctx, tg, job, fixture, []string{matchedHost, untouchedHost})

	csv := buildDeviceTimezoneCSV([]deviceTimezoneCSVRow{
		{Hostname: matchedHost, Timezone: "Asia/Tokyo"},
		{Hostname: missingHost, Timezone: "America/Chicago"},
	})
	uploadArtifactsObject(t, ctx, job.GetCloud(t), filePath, csv)

	seed, err := fixtures.SeedDeviceTimezoneImportRequest(
		ctx, GormDB(), fixture,
		importRequestID,
		fmt.Sprintf("import-partial-%s", uuid.NewString()[:8]),
		filePath,
		"devices.csv",
		true,
		fixtures.ImportStatusRequested,
	)
	if err != nil {
		t.Fatalf("seed import_request: %v", err)
	}
	if seed.ID != importRequestID {
		t.Fatalf("seed id mismatch: got %s want %s", seed.ID, importRequestID)
	}

	runImportRequestProcessorJob(t, ctx, tg, job)
	refreshIndex(t, ctx, openSearch, devicesIndexName(fixture.TenantID.String()))

	row := getImportRequestRow(t, ctx, importRequestID)
	if row.Status != "COMPLETED" {
		t.Fatalf("import_request status = %q, want COMPLETED (error=%q)", row.Status, row.ErrorMessage)
	}

	stats := parseImportRequestStats(t, row.Stats)
	if stats.TotalRows != 2 || stats.ProcessedRows != 1 || stats.FailedRows != 1 {
		t.Fatalf("stats = %+v, want total=2 processed=1 failed=1", stats)
	}

	matched := getDeviceDocument(t, ctx, openSearch, fixture.TenantID.String(), matchedHost)
	assertStringField(t, matched, "device_timezone", "Asia/Tokyo")
	assertStringField(t, matched, "timezone_updated_by", fixture.ActorID.String())

	untouched := getDeviceDocument(t, ctx, openSearch, fixture.TenantID.String(), untouchedHost)
	if _, ok := untouched["device_timezone"].(string); ok {
		t.Fatalf("untouched device should not have device_timezone set, got %#v", untouched["device_timezone"])
	}
	if untouched["timezone_updated_by"] != nil && untouched["timezone_updated_by"] != "" {
		t.Fatalf("untouched device should not have timezone_updated_by set, got %#v", untouched["timezone_updated_by"])
	}

	_, err = openSearch.GetDocument(ctx, devicesIndexName(fixture.TenantID.String()), missingDeviceID)
	if err == nil {
		t.Fatalf("expected missing device %q to remain absent from index", missingDeviceID)
	}
}

func TestImportRequestProcessorClearsDeviceTimezoneWithEmptyColumn(t *testing.T) {
	ctx := context.Background()
	tg := pramaan.NewGoTestLogger(t)
	job := JobPramaan()
	openSearch := job.GetOpenSearch(t)

	fixture := seedImportRequestFixture(t)
	const host = "clear-timezone-host"

	importRequestID := uuid.New()
	filePath := fixtures.BuildImportFilePath(fixture.TenantID, importRequestID, "devices.csv")
	t.Cleanup(func() {
		cleanupImportRequestTest(t, ctx, openSearch, fixture.TenantID.String(), importRequestID)
	})

	seedDeviceWithAggregation(t, ctx, tg, job, fixture, host)

	setCSV := buildDeviceTimezoneCSV([]deviceTimezoneCSVRow{
		{Hostname: host, Timezone: "Europe/London"},
	})
	setFilePath := fixtures.BuildImportFilePath(fixture.TenantID, uuid.New(), "devices-set.csv")
	uploadArtifactsObject(t, ctx, job.GetCloud(t), setFilePath, setCSV)

	setImportID := uuid.New()
	seed, err := fixtures.SeedDeviceTimezoneImportRequest(
		ctx, GormDB(), fixture,
		setImportID,
		fmt.Sprintf("import-set-%s", uuid.NewString()[:8]),
		setFilePath,
		"devices-set.csv",
		true,
		fixtures.ImportStatusRequested,
	)
	if err != nil {
		t.Fatalf("seed set import_request: %v", err)
	}
	if seed.ID != setImportID {
		t.Fatalf("seed id mismatch: got %s want %s", seed.ID, setImportID)
	}

	runImportRequestProcessorJob(t, ctx, tg, job)
	refreshIndex(t, ctx, openSearch, devicesIndexName(fixture.TenantID.String()))

	device := getDeviceDocument(t, ctx, openSearch, fixture.TenantID.String(), host)
	assertStringField(t, device, "device_timezone", "Europe/London")

	clearCSV := buildDeviceTimezoneCSV([]deviceTimezoneCSVRow{
		{Hostname: host, Timezone: ""},
	})
	uploadArtifactsObject(t, ctx, job.GetCloud(t), filePath, clearCSV)

	seed, err = fixtures.SeedDeviceTimezoneImportRequest(
		ctx, GormDB(), fixture,
		importRequestID,
		fmt.Sprintf("import-clear-%s", uuid.NewString()[:8]),
		filePath,
		"devices.csv",
		true,
		fixtures.ImportStatusRequested,
	)
	if err != nil {
		t.Fatalf("seed clear import_request: %v", err)
	}
	if seed.ID != importRequestID {
		t.Fatalf("seed id mismatch: got %s want %s", seed.ID, importRequestID)
	}

	runImportRequestProcessorJob(t, ctx, tg, job)
	refreshIndex(t, ctx, openSearch, devicesIndexName(fixture.TenantID.String()))

	row := getImportRequestRow(t, ctx, importRequestID)
	if row.Status != "COMPLETED" {
		t.Fatalf("import_request status = %q, want COMPLETED (error=%q)", row.Status, row.ErrorMessage)
	}

	stats := parseImportRequestStats(t, row.Stats)
	if stats.TotalRows != 1 || stats.ProcessedRows != 1 || stats.FailedRows != 0 {
		t.Fatalf("stats = %+v, want total=1 processed=1 failed=0", stats)
	}

	device = getDeviceDocument(t, ctx, openSearch, fixture.TenantID.String(), host)
	assertStringField(t, device, "device_timezone", "")
	assertStringField(t, device, "timezone_updated_by", fixture.ActorID.String())
	if got, ok := device["timezone_updated_at"].(float64); !ok || got <= 0 {
		t.Fatalf("timezone_updated_at = %#v, want positive timestamp", device["timezone_updated_at"])
	}
}
