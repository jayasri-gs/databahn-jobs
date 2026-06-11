package replayFilesReport

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/auditReport/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestGetReplayJobIdFromRequest(t *testing.T) {
	jobId := uuid.New().String()
	configJSON, err := json.Marshal(reportConfiguration{
		ReplayFilesReportConfig: &replayFilesReportConfig{ReplayJobId: jobId},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}

	got, err := getReplayJobIdFromRequest(models.AuditReport{
		ReportConfiguration: datatypes.JSON(configJSON),
	})
	if err != nil {
		t.Fatalf("getReplayJobIdFromRequest returned error: %v", err)
	}
	if got != jobId {
		t.Fatalf("replayJobId = %q, want %q", got, jobId)
	}
}

func TestGetReplayJobIdFromRequest_missingConfig(t *testing.T) {
	_, err := getReplayJobIdFromRequest(models.AuditReport{})
	if err == nil {
		t.Fatal("expected error for missing report configuration")
	}
}

func TestGetReplayJobIdFromRequest_missingReplayJobId(t *testing.T) {
	configJSON, err := json.Marshal(reportConfiguration{
		ReplayFilesReportConfig: &replayFilesReportConfig{},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}

	_, err = getReplayJobIdFromRequest(models.AuditReport{
		ReportConfiguration: datatypes.JSON(configJSON),
	})
	if err == nil {
		t.Fatal("expected error for missing replay job id")
	}
}

func TestSizeOrLinesValue_prefersSizeWhenPresent(t *testing.T) {
	columns := []string{"size", "lines"}
	values := scanValues(columns, map[string]string{
		"size":  "1024",
		"lines": "500",
	})
	got := sizeOrLinesValue(values, columnIndexFor(columns))
	if got != "1024" {
		t.Fatalf("sizeOrLinesValue = %q, want %q", got, "1024")
	}
}

func TestSizeOrLinesValue_fallsBackToLinesWhenSizeMissing(t *testing.T) {
	columns := []string{"size", "lines"}
	values := scanValues(columns, map[string]string{
		"size":  "",
		"lines": "500",
	})
	got := sizeOrLinesValue(values, columnIndexFor(columns))
	if got != "500" {
		t.Fatalf("sizeOrLinesValue = %q, want %q", got, "500")
	}
}

func scanValues(columns []string, columnValues map[string]string) []interface{} {
	values := make([]interface{}, len(columns))
	for i, column := range columns {
		raw := sql.RawBytes(columnValues[column])
		values[i] = &raw
	}
	return values
}

func columnIndexFor(columns []string) map[string]int {
	index := make(map[string]int, len(columns))
	for i, column := range columns {
		index[column] = i
	}
	return index
}

func TestBuildQuery_includesJobAndTenantFilters(t *testing.T) {
	jobId := "6e3a0fe3-872b-43aa-a25e-75f7bdd7e408"
	tenantId := "c0ffee00-c0ff-ee00-c0ff-ee00c0ffee00"

	query := buildQuery(jobId, tenantId)
	if query == "" {
		t.Fatal("expected non-empty query")
	}
	for _, part := range []string{jobId, tenantId, "replay_job_executions", "replay_jobs", "e.lines"} {
		if !strings.Contains(query, part) {
			t.Fatalf("query missing %q: %s", part, query)
		}
	}
}
