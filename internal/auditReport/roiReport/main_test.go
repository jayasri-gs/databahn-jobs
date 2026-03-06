package roiReport

import (
	"encoding/csv"
	"os"
	"testing"
)

func TestWriteRowsToFileForROIReport(t *testing.T) {
	tests := []struct {
		name              string
		responses         []Response
		destMap           map[string]string
		sourceMap         map[string]string
		expectedRowCount  int
		expectedHeaders   []string
		expectedFirstRow  []string
		expectFallbackIDs bool
	}{
		{
			name: "single row with resolved names",
			responses: []Response{
				{
					LogSourceId:              "src-001",
					DestinationId:            "dest-001",
					IncomingBytes:            "1048576",
					OutgoingBytes:            "524288",
					ByteReductionPercentage:  "50.00",
					IncomingEvents:           "10000",
					OutgoingEvents:           "6000",
					EventReductionPercentage: "40.00",
				},
			},
			destMap:          map[string]string{"dest-001": "Splunk-Prod"},
			sourceMap:        map[string]string{"src-001": "Firewall-Logs"},
			expectedRowCount: 1,
			expectedHeaders:  []string{"Source", "Destination", "Incoming Data", "Outgoing Data", "Reduction percentage", "Incoming Data (Bytes)", "Outgoing Data (Bytes)", "Data Reduction %"},
			expectedFirstRow: []string{"Firewall-Logs", "Splunk-Prod", "10000", "6000", "40.00", "1048576", "524288", "50.00"},
		},
		{
			name: "fallback to IDs when names not found",
			responses: []Response{
				{
					LogSourceId:              "src-unknown",
					DestinationId:            "dest-unknown",
					IncomingBytes:            "2000",
					OutgoingBytes:            "1000",
					ByteReductionPercentage:  "50.00",
					IncomingEvents:           "500",
					OutgoingEvents:           "250",
					EventReductionPercentage: "50.00",
				},
			},
			destMap:          map[string]string{},
			sourceMap:        map[string]string{},
			expectedRowCount: 1,
			expectedFirstRow: []string{"src-unknown", "dest-unknown", "500", "250", "50.00", "2000", "1000", "50.00"},
		},
		{
			name:             "empty response produces no rows",
			responses:        []Response{},
			destMap:          map[string]string{},
			sourceMap:        map[string]string{},
			expectedRowCount: 0,
		},
		{
			name: "multiple rows",
			responses: []Response{
				{
					LogSourceId:              "src-001",
					DestinationId:            "dest-001",
					IncomingBytes:            "1000",
					OutgoingBytes:            "500",
					ByteReductionPercentage:  "50.00",
					IncomingEvents:           "100",
					OutgoingEvents:           "60",
					EventReductionPercentage: "40.00",
				},
				{
					LogSourceId:              "src-002",
					DestinationId:            "dest-002",
					IncomingBytes:            "2000",
					OutgoingBytes:            "200",
					ByteReductionPercentage:  "90.00",
					IncomingEvents:           "500",
					OutgoingEvents:           "50",
					EventReductionPercentage: "90.00",
				},
			},
			destMap:          map[string]string{"dest-001": "Splunk", "dest-002": "S3"},
			sourceMap:        map[string]string{"src-001": "FW-Logs", "src-002": "App-Logs"},
			expectedRowCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "roi-test-*.csv")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())

			writer := csv.NewWriter(tmpFile)

			headers := []string{"Source", "Destination", "Incoming Data", "Outgoing Data", "Reduction percentage", "Incoming Data (Bytes)", "Outgoing Data (Bytes)", "Data Reduction %"}
			if err := writer.Write(headers); err != nil {
				t.Fatalf("failed to write headers: %v", err)
			}

			err = writeRowsToFileForROIReport(tt.responses, tt.destMap, tt.sourceMap, writer)
			if err != nil {
				t.Fatalf("writeRowsToFileForROIReport returned error: %v", err)
			}
			tmpFile.Close()

			readFile, err := os.Open(tmpFile.Name())
			if err != nil {
				t.Fatalf("failed to open temp file for reading: %v", err)
			}
			defer readFile.Close()

			reader := csv.NewReader(readFile)
			records, err := reader.ReadAll()
			if err != nil {
				t.Fatalf("failed to read CSV: %v", err)
			}

			// records[0] = headers, rest are data rows
			dataRows := records[1:]
			if len(dataRows) != tt.expectedRowCount {
				t.Errorf("expected %d data rows, got %d", tt.expectedRowCount, len(dataRows))
			}

			if tt.expectedHeaders != nil {
				if len(records[0]) != len(tt.expectedHeaders) {
					t.Errorf("expected %d columns, got %d", len(tt.expectedHeaders), len(records[0]))
				}
				for i, h := range tt.expectedHeaders {
					if i < len(records[0]) && records[0][i] != h {
						t.Errorf("header[%d] = %q, want %q", i, records[0][i], h)
					}
				}
			}

			if tt.expectedFirstRow != nil && len(dataRows) > 0 {
				for i, val := range tt.expectedFirstRow {
					if i < len(dataRows[0]) && dataRows[0][i] != val {
						t.Errorf("row[0][%d] = %q, want %q", i, dataRows[0][i], val)
					}
				}
			}

			// Verify every row has exactly 8 columns
			for rowIdx, row := range dataRows {
				if len(row) != 8 {
					t.Errorf("row %d has %d columns, want 8", rowIdx, len(row))
				}
			}
		})
	}
}

func TestWriteRowsColumnOrder(t *testing.T) {
	responses := []Response{
		{
			LogSourceId:              "src-1",
			DestinationId:            "dest-1",
			IncomingBytes:            "BYTE_IN",
			OutgoingBytes:            "BYTE_OUT",
			ByteReductionPercentage:  "BYTE_RED",
			IncomingEvents:           "EVT_IN",
			OutgoingEvents:           "EVT_OUT",
			EventReductionPercentage: "EVT_RED",
		},
	}
	destMap := map[string]string{"dest-1": "DEST_NAME"}
	sourceMap := map[string]string{"src-1": "SRC_NAME"}

	tmpFile, err := os.CreateTemp("", "roi-order-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	_ = writer.Write([]string{"h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8"})
	err = writeRowsToFileForROIReport(responses, destMap, sourceMap, writer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tmpFile.Close()

	readFile, _ := os.Open(tmpFile.Name())
	defer readFile.Close()
	reader := csv.NewReader(readFile)
	records, _ := reader.ReadAll()
	row := records[1]

	expectedOrder := []string{
		"SRC_NAME",  // col 0: Source
		"DEST_NAME", // col 1: Destination
		"EVT_IN",    // col 2: Incoming Data (events - existing)
		"EVT_OUT",   // col 3: Outgoing Data (events - existing)
		"EVT_RED",   // col 4: Reduction percentage (events - existing)
		"BYTE_IN",   // col 5: Incoming Data (Bytes) (new)
		"BYTE_OUT",  // col 6: Outgoing Data (Bytes) (new)
		"BYTE_RED",  // col 7: Data Reduction % (new)
	}

	for i, expected := range expectedOrder {
		if row[i] != expected {
			t.Errorf("column %d = %q, want %q", i, row[i], expected)
		}
	}
}
