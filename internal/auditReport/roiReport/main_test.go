package roiReport

import (
	"encoding/csv"
	"os"
	"testing"
)

var csvHeaders = []string{
	"Source", "Destination",
	"Incoming Data", "Outgoing Data", "Reduction percentage",
	"Incoming Data (Bytes)", "Outgoing Data (Bytes)", "Data Reduction %",
}

const expectedColumnCount = 8

func writeTestCSV(t *testing.T, responses []Response, destMap, sourceMap map[string]string) [][]string {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "roi-test-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	if err := writer.Write(csvHeaders); err != nil {
		t.Fatalf("failed to write headers: %v", err)
	}

	if err := writeRowsToFileForROIReport(responses, destMap, sourceMap, writer); err != nil {
		t.Fatalf("writeRowsToFileForROIReport returned error: %v", err)
	}
	tmpFile.Close()

	readFile, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer readFile.Close()

	records, err := csv.NewReader(readFile).ReadAll()
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}
	return records
}

func assertRowValues(t *testing.T, row []string, expected []string) {
	t.Helper()
	for i, val := range expected {
		if i < len(row) && row[i] != val {
			t.Errorf("column[%d] = %q, want %q", i, row[i], val)
		}
	}
}

func TestWriteRows_SingleRowWithNames(t *testing.T) {
	records := writeTestCSV(t,
		[]Response{{
			LogSourceId: "src-001", DestinationId: "dest-001",
			IncomingBytes: "1048576", OutgoingBytes: "524288", ByteReductionPercentage: "50.00",
			IncomingEvents: "10000", OutgoingEvents: "6000", EventReductionPercentage: "40.00",
		}},
		map[string]string{"dest-001": "Splunk-Prod"},
		map[string]string{"src-001": "Firewall-Logs"},
	)

	if len(records) != 2 {
		t.Fatalf("expected 2 records (header + 1 row), got %d", len(records))
	}
	assertRowValues(t, records[1], []string{
		"Firewall-Logs", "Splunk-Prod", "10000", "6000", "40.00", "1048576", "524288", "50.00",
	})
}

func TestWriteRows_FallbackToIDs(t *testing.T) {
	records := writeTestCSV(t,
		[]Response{{
			LogSourceId: "src-unknown", DestinationId: "dest-unknown",
			IncomingBytes: "2000", OutgoingBytes: "1000", ByteReductionPercentage: "50.00",
			IncomingEvents: "500", OutgoingEvents: "250", EventReductionPercentage: "50.00",
		}},
		map[string]string{},
		map[string]string{},
	)

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	assertRowValues(t, records[1], []string{
		"src-unknown", "dest-unknown", "500", "250", "50.00", "2000", "1000", "50.00",
	})
}

func TestWriteRows_EmptyResponse(t *testing.T) {
	records := writeTestCSV(t, []Response{}, map[string]string{}, map[string]string{})
	if len(records) != 1 {
		t.Errorf("expected 1 record (header only), got %d", len(records))
	}
}

func TestWriteRows_MultipleRows(t *testing.T) {
	records := writeTestCSV(t,
		[]Response{
			{
				LogSourceId: "src-001", DestinationId: "dest-001",
				IncomingBytes: "1000", OutgoingBytes: "500", ByteReductionPercentage: "50.00",
				IncomingEvents: "100", OutgoingEvents: "60", EventReductionPercentage: "40.00",
			},
			{
				LogSourceId: "src-002", DestinationId: "dest-002",
				IncomingBytes: "2000", OutgoingBytes: "200", ByteReductionPercentage: "90.00",
				IncomingEvents: "500", OutgoingEvents: "50", EventReductionPercentage: "90.00",
			},
		},
		map[string]string{"dest-001": "Splunk", "dest-002": "S3"},
		map[string]string{"src-001": "FW-Logs", "src-002": "App-Logs"},
	)

	dataRows := records[1:]
	if len(dataRows) != 2 {
		t.Errorf("expected 2 data rows, got %d", len(dataRows))
	}
}

func TestWriteRows_ColumnCount(t *testing.T) {
	records := writeTestCSV(t,
		[]Response{{
			LogSourceId: "src-1", DestinationId: "dest-1",
			IncomingBytes: "100", OutgoingBytes: "50", ByteReductionPercentage: "50.00",
			IncomingEvents: "10", OutgoingEvents: "5", EventReductionPercentage: "50.00",
		}},
		map[string]string{"dest-1": "D"}, map[string]string{"src-1": "S"},
	)

	for i, row := range records {
		if len(row) != expectedColumnCount {
			t.Errorf("row %d has %d columns, want %d", i, len(row), expectedColumnCount)
		}
	}
}

func TestWriteRows_HeaderNames(t *testing.T) {
	records := writeTestCSV(t, []Response{}, map[string]string{}, map[string]string{})
	assertRowValues(t, records[0], csvHeaders)
}

func TestWriteRows_ColumnOrder(t *testing.T) {
	records := writeTestCSV(t,
		[]Response{{
			LogSourceId: "src-1", DestinationId: "dest-1",
			IncomingBytes: "BYTE_IN", OutgoingBytes: "BYTE_OUT", ByteReductionPercentage: "BYTE_RED",
			IncomingEvents: "EVT_IN", OutgoingEvents: "EVT_OUT", EventReductionPercentage: "EVT_RED",
		}},
		map[string]string{"dest-1": "DEST_NAME"},
		map[string]string{"src-1": "SRC_NAME"},
	)

	assertRowValues(t, records[1], []string{
		"SRC_NAME",  // col 0: Source
		"DEST_NAME", // col 1: Destination
		"EVT_IN",    // col 2: Incoming Data (events - existing)
		"EVT_OUT",   // col 3: Outgoing Data (events - existing)
		"EVT_RED",   // col 4: Reduction percentage (events - existing)
		"BYTE_IN",   // col 5: Incoming Data (Bytes) (new)
		"BYTE_OUT",  // col 6: Outgoing Data (Bytes) (new)
		"BYTE_RED",  // col 7: Data Reduction % (new)
	})
}
