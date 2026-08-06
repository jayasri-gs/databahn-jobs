package importrequest

import (
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/importrequest/models"
	"github.com/databahn-ai/databahn-jobs/internal/insights"
	"go.uber.org/zap"
)

func TestParseDeviceTimezoneCSVWithHeaders(t *testing.T) {
	csv := "hostname,device_timezone\n" +
		"host-1,America/New_York\n" +
		"host-2,UTC\n"

	rows, skipped, err := parseDeviceTimezoneCSV([]byte(csv), true)
	if err != nil {
		t.Fatalf("parseDeviceTimezoneCSV() error = %v", err)
	}
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	if rows[0].Hostname != "host-1" || rows[0].Timezone != "America/New_York" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
}

func TestParseDeviceTimezoneCSVWithoutHeaders(t *testing.T) {
	csv := "host-1,America/New_York\n"

	rows, _, err := parseDeviceTimezoneCSV([]byte(csv), false)
	if err != nil {
		t.Fatalf("parseDeviceTimezoneCSV() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
}

func TestParseDeviceTimezoneCSVAcceptsEmptyTimezoneColumn(t *testing.T) {
	csv := "hostname,device_timezone\n" +
		"host-1,\n"

	rows, skipped, err := parseDeviceTimezoneCSV([]byte(csv), true)
	if err != nil {
		t.Fatalf("parseDeviceTimezoneCSV() error = %v", err)
	}
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	if rows[0].Hostname != "host-1" || rows[0].Timezone != "" {
		t.Fatalf("unexpected row: %+v", rows[0])
	}
}

func TestParseDeviceTimezoneCSVAcceptsArbitraryHeaders(t *testing.T) {
	csv := "name,timezone\n" +
		"host-1,America/New_York\n"

	rows, skipped, err := parseDeviceTimezoneCSV([]byte(csv), true)
	if err != nil {
		t.Fatalf("parseDeviceTimezoneCSV() error = %v", err)
	}
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	if rows[0].Hostname != "host-1" || rows[0].Timezone != "America/New_York" {
		t.Fatalf("unexpected row: %+v", rows[0])
	}
}

func TestValidateHostname(t *testing.T) {
	if err := validateHostname("host-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := validateHostname(""); err == nil {
		t.Fatal("expected empty hostname error")
	}
	if err := validateHostname("2001:db8::1"); err != nil {
		t.Fatalf("hostname with colon should be valid, got: %v", err)
	}
}

func TestValidateTimezone(t *testing.T) {
	valid := []string{"UTC", "America/New_York", "Europe/London", "Asia/Kolkata", "US/Eastern"}
	for _, tz := range valid {
		if err := insights.ValidateIANATimezone(tz); err != nil {
			t.Fatalf("ValidateIANATimezone(%q) error = %v", tz, err)
		}
	}

	invalid := []string{"", "EST", "Not/AZone", "+05:30"}
	for _, tz := range invalid {
		if err := insights.ValidateIANATimezone(tz); err == nil {
			t.Fatalf("ValidateIANATimezone(%q) expected error", tz)
		}
	}
}

func TestTimezoneUpdateReasonForImport(t *testing.T) {
	if got := timezoneUpdateReasonForImport("America/New_York"); got != insights.TimezoneUpdateReasonManual {
		t.Fatalf("set timezone reason = %q, want %q", got, insights.TimezoneUpdateReasonManual)
	}
	if got := timezoneUpdateReasonForImport(""); got != "" {
		t.Fatalf("clear timezone reason = %q, want empty", got)
	}
	if got := timezoneUpdateReasonForImport("  "); got != "" {
		t.Fatalf("blank timezone reason = %q, want empty", got)
	}
}

func TestApplyBulkOutcomes(t *testing.T) {
	stats := models.ImportStats{}
	batch := []csvRow{
		{RowNumber: 2, Hostname: "host-1", Timezone: "UTC"},
		{RowNumber: 3, Hostname: "host-2", Timezone: "UTC"},
	}
	outcomes := []bulkItemOutcome{
		{deviceID: "tenant-1:host-1", status: 200},
		{deviceID: "tenant-1:host-2", status: 404, reason: "document_missing_exception"},
	}

	applyBulkOutcomes(zap.NewNop(), &stats, "tenant-1", batch, outcomes)

	if stats.ProcessedRows != 1 {
		t.Fatalf("ProcessedRows = %d, want 1", stats.ProcessedRows)
	}
	if stats.FailedRows != 1 {
		t.Fatalf("FailedRows = %d, want 1", stats.FailedRows)
	}
}
