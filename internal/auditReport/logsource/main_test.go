package logsource

import (
	"bytes"
	"encoding/csv"
	"slices"
	"strings"
	"testing"
)

func TestFormatEventCountReadable(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"0", "0"},
		{"999", "999"},
		{"1000", "1K"},
		{"1500", "1.5K"},
		{"259380", "259.38K"},
		{"1000000", "1M"},
		{"1500000", "1.5M"},
		{"1000000000", "1B"},
		{"1092483409", "1.09B"},
		{"1102378492", "1.1B"},
		{"1234567890", "1.23B"},
		{"38472918", "38.47M"},
		{"4728391", "4.73M"},
		{"987654321", "987.65M"},
		{"942817", "942.82K"},
		{"not-a-number", "not-a-number"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := formatEventCountReadable(tt.in)
			if got != tt.want {
				t.Errorf("formatEventCountReadable(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestLogSourceReportExtraHeaderColumns(t *testing.T) {
	want := []string{
		"ingestion_stats", "destination_stats",
		"ingestion_stats_formatted", "destination_stats_formatted",
	}
	if !slices.Equal(logSourceReportExtraHeaderColumns, want) {
		t.Fatalf("logSourceReportExtraHeaderColumns = %v, want %v", logSourceReportExtraHeaderColumns, want)
	}
}

func TestAppendLogSourceStatColumns(t *testing.T) {
	lsID := "118aca7c-048d-4c62-98ee-0fad6bafeb20"
	ingestion := map[string]string{lsID: "259380"}
	destBySource := map[string]map[string]string{
		lsID: {
			"dest-uuid-1": "1000",
			"dest-uuid-2": "1500000",
		},
	}
	destNames := map[string]string{
		"dest-uuid-1": "Dest A",
		"dest-uuid-2": "Dest B",
	}

	row := appendLogSourceStatColumns([]string{"prefix"}, lsID, ingestion, destBySource, destNames)

	if len(row) != 5 {
		t.Fatalf("len(row) = %d, want 5 (prefix + 4 stat columns)", len(row))
	}
	if row[0] != "prefix" {
		t.Errorf("row[0] = %q, want prefix", row[0])
	}
	if row[1] != "259380" {
		t.Errorf("ingestion_stats = %q", row[1])
	}

	// Map iteration order is undefined; compare destination columns as sorted lines.
	rawLines := strings.Split(row[2], "\n")
	gotRaw := slices.Clone(rawLines)
	slices.Sort(gotRaw)
	wantRaw := []string{"Dest A: 1000", "Dest B: 1500000"}
	slices.Sort(wantRaw)
	if !slices.Equal(gotRaw, wantRaw) {
		t.Errorf("destination_stats lines\ngot:  %v\nwant: %v", gotRaw, wantRaw)
	}

	if row[3] != "259.38K" {
		t.Errorf("ingestion_stats_formatted = %q, want 259.38K", row[3])
	}

	fmtLines := strings.Split(row[4], "\n")
	gotFmt := slices.Clone(fmtLines)
	slices.Sort(gotFmt)
	wantFmt := []string{"Dest A: 1K", "Dest B: 1.5M"}
	slices.Sort(wantFmt)
	if !slices.Equal(gotFmt, wantFmt) {
		t.Errorf("destination_stats_formatted lines\ngot:  %v\nwant: %v", gotFmt, wantFmt)
	}
}

// TestLogSourceReportCSV_dummyRoundTrip writes a minimal CSV (no DB/OpenSearch) using the same
// header list and appendLogSourceStatColumns as production, then reads it back to verify shape and values.
func TestLogSourceReportCSV_dummyRoundTrip(t *testing.T) {
	lsID := "118aca7c-048d-4c62-98ee-0fad6bafeb20"
	ingestion := map[string]string{lsID: "259380"}
	destBySource := map[string]map[string]string{
		lsID: {"dest-one": "1000"},
	}
	destNames := map[string]string{"dest-one": "Dest A"}

	sqlCols := []string{"id", "name"}
	header := append(slices.Clone(sqlCols), logSourceReportExtraHeaderColumns...)

	row := []string{lsID, "dummy-source-name"}
	row = appendLogSourceStatColumns(row, lsID, ingestion, destBySource, destNames)

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(header); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if err := w.Write(row); err != nil {
		t.Fatalf("write row: %v", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatalf("writer: %v", err)
	}

	// Run with: go test ./internal/auditReport/logsource/... -run TestLogSourceReportCSV_dummyRoundTrip -v
	t.Logf("CSV written (same shape as report file):\n%s", buf.String())

	r := csv.NewReader(strings.NewReader(buf.String()))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("want 2 records (header + data), got %d", len(records))
	}
	if !slices.Equal(records[0], header) {
		t.Errorf("header mismatch\ngot:  %v\nwant: %v", records[0], header)
	}
	got := records[1]
	want := []string{
		lsID,
		"dummy-source-name",
		"259380",
		"Dest A: 1000",
		"259.38K",
		"Dest A: 1K",
	}
	if !slices.Equal(got, want) {
		t.Errorf("data row mismatch\ngot:  %v\nwant: %v", got, want)
	}
}

// TestLogSourceReportCSV_multilineDestinationField ensures csv.Writer/Reader preserve newline-separated destination lines in one cell.
func TestLogSourceReportCSV_multilineDestinationField(t *testing.T) {
	lsID := "118aca7c-048d-4c62-98ee-0fad6bafeb20"
	ingestion := map[string]string{lsID: "100"}
	destBySource := map[string]map[string]string{
		lsID: {"a": "10", "b": "20"},
	}
	destNames := map[string]string{"a": "A", "b": "B"}

	row := []string{lsID, "x"}
	row = appendLogSourceStatColumns(row, lsID, ingestion, destBySource, destNames)

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(append([]string{"id", "name"}, logSourceReportExtraHeaderColumns...))
	_ = w.Write(row)
	w.Flush()

	r := csv.NewReader(strings.NewReader(buf.String()))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records", len(records))
	}
	raw := records[1][3]
	fmt := records[1][5]
	if records[1][4] != "100" {
		t.Errorf("ingestion_stats_formatted = %q, want 100", records[1][4])
	}
	rawLines := strings.Split(raw, "\n")
	slices.Sort(rawLines)
	wantRaw := []string{"A: 10", "B: 20"}
	slices.Sort(wantRaw)
	if !slices.Equal(rawLines, wantRaw) {
		t.Errorf("destination_stats raw: %v", rawLines)
	}
	fmtLines := strings.Split(fmt, "\n")
	slices.Sort(fmtLines)
	wantFmt := []string{"A: 10", "B: 20"}
	slices.Sort(wantFmt)
	if !slices.Equal(fmtLines, wantFmt) {
		t.Errorf("destination_stats formatted: %v", fmtLines)
	}
}

func TestAppendLogSourceStatColumns_emptyIngestion(t *testing.T) {
	lsID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	row := appendLogSourceStatColumns(nil, lsID, map[string]string{}, map[string]map[string]string{}, nil)
	if len(row) != 4 {
		t.Fatalf("len = %d, want 4", len(row))
	}
	for i, name := range []string{"ingestion_stats", "destination_stats", "ingestion_stats_formatted", "destination_stats_formatted"} {
		if row[i] != "" {
			t.Errorf("%s: got %q, want empty", name, row[i])
		}
	}
}
