package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ADX .export defaults. sizeLimit is the per-blob cap Kusto enforces (1 GiB maximum).
const (
	adxExportSizeLimitBytes = 1073741824
	adxNamePrefixPrefix     = "databahn_export_"
)

// ADX operation states returned by `.show operations`.
const (
	adxStateCompleted  = "COMPLETED"
	adxStateInProgress = "INPROGRESS"
	adxStateScheduled  = "SCHEDULED"
	adxStateThrottled  = "THROTTLED"
	adxStateFailed     = "FAILED"
	adxStateBadInput   = "BADINPUT"
	adxStateAbandoned  = "ABANDONED"
	adxStateCanceled   = "CANCELED"
	adxStateCancelled  = "CANCELLED"
)

// ADXNamePrefix returns the stable blob name prefix for a report's exported files.
// Stable across retries so a resumed job finds the blobs written by the first attempt.
func ADXNamePrefix(reportID string) string {
	short := strings.ReplaceAll(reportID, "-", "")
	if len(short) > 12 {
		short = short[:12]
	}
	if short == "" {
		short = "report"
	}
	return adxNamePrefixPrefix + short
}

// ADXExportFormat maps an internal export format onto the Kusto `.export to <format>` token.
// Anything Kusto cannot emit natively falls back to parquet, which the pipeline decodes locally.
func ADXExportFormat(opts UnloadOptions) string {
	switch opts.Format {
	case "csv":
		return "csv"
	case "tsv":
		return "tsv"
	case "json":
		return "json"
	default:
		return "parquet"
	}
}

// BuildADXExportCommand renders an async `.export` control command.
//
// The query is prefixed with `set notruncation;` because the default 500k-row query
// result truncation applies to `.export` too and would silently cap large exports.
// Headers are never written by Kusto (includeHeaders=none): blob name ordering is
// lexical, so a header in "the first file" is not reliably the first file we read.
// The pipeline prepends its own header built from the query schema instead.
func BuildADXExportCommand(kql, storageConnectionString, format, namePrefix string) string {
	var props []string
	props = append(props, fmt.Sprintf("namePrefix=%s", quoteKustoString(namePrefix)))
	if format == "csv" || format == "tsv" {
		props = append(props, "includeHeaders=none", "encoding=UTF8NoBOM")
	}
	props = append(props, "sizeLimit="+strconv.Itoa(adxExportSizeLimitBytes))

	return fmt.Sprintf(
		".export async to %s (h@%s)\nwith (%s)\n<| set notruncation;\n%s",
		format,
		quoteKustoString(storageConnectionString),
		strings.Join(props, ", "),
		strings.TrimSpace(kql),
	)
}

// BuildADXShowOperationCommand returns the command polling an async operation's state.
func BuildADXShowOperationCommand(operationID string) string {
	return fmt.Sprintf(".show operations %s", quoteKustoString(operationID))
}

// BuildADXShowOperationDetailsCommand returns the command listing the blobs an export wrote.
// Valid only once the operation has completed.
func BuildADXShowOperationDetailsCommand(operationID string) string {
	return fmt.Sprintf(".show operation %s details", quoteKustoString(operationID))
}

// BuildADXCancelOperationCommand returns the command cancelling a running operation.
func BuildADXCancelOperationCommand(operationID string) string {
	return fmt.Sprintf(".cancel operation %s", quoteKustoString(operationID))
}

// BuildADXSchemaQuery returns a KQL query yielding the export's column names in ordinal order.
func BuildADXSchemaQuery(kql string) string {
	return strings.TrimSpace(kql) + "\n| getschema\n| sort by ColumnOrdinal asc\n| project ColumnName"
}

// MapADXOperationState translates a Kusto operation state into a pipeline query state.
func MapADXOperationState(state string) string {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case adxStateCompleted:
		return QueryStateSucceeded
	case adxStateInProgress:
		return QueryStateRunning
	case adxStateScheduled, adxStateThrottled:
		return QueryStateQueued
	case adxStateCanceled, adxStateCancelled, adxStateAbandoned:
		return QueryStateCancelled
	case adxStateFailed, adxStateBadInput:
		return QueryStateFailed
	default:
		return QueryStateFailed
	}
}

// quoteKustoString renders a Go string as a Kusto double-quoted literal.
func quoteKustoString(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return `"` + replacer.Replace(s) + `"`
}

// ---------------------------------------------------------------------------
// v1 REST response parsing
// ---------------------------------------------------------------------------

type kustoColumn struct {
	ColumnName string `json:"ColumnName"`
	DataType   string `json:"DataType"`
	ColumnType string `json:"ColumnType"`
}

type kustoTable struct {
	TableName string              `json:"TableName"`
	Columns   []kustoColumn       `json:"Columns"`
	Rows      [][]json.RawMessage `json:"Rows"`
}

type kustoV1Response struct {
	Tables []kustoTable `json:"Tables"`
}

// parseKustoV1Response unmarshals a /v1/rest/{mgmt,query} response body.
func parseKustoV1Response(body []byte) (*kustoV1Response, error) {
	var resp kustoV1Response
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse kusto response: %w", err)
	}
	if len(resp.Tables) == 0 {
		return nil, fmt.Errorf("kusto response contained no tables")
	}
	return &resp, nil
}

// primaryTable returns the first table carrying the command result.
func (r *kustoV1Response) primaryTable() *kustoTable {
	if r == nil || len(r.Tables) == 0 {
		return nil
	}
	return &r.Tables[0]
}

func (t *kustoTable) columnIndex(name string) int {
	if t == nil {
		return -1
	}
	for i, c := range t.Columns {
		if strings.EqualFold(c.ColumnName, name) {
			return i
		}
	}
	return -1
}

// stringColumn returns every non-empty value of a column, in row order.
func (t *kustoTable) stringColumn(name string) []string {
	idx := t.columnIndex(name)
	if idx < 0 {
		return nil
	}
	values := make([]string, 0, len(t.Rows))
	for _, row := range t.Rows {
		if idx >= len(row) {
			continue
		}
		if v := rawToString(row[idx]); v != "" {
			values = append(values, v)
		}
	}
	return values
}

func (t *kustoTable) firstString(name string) string {
	values := t.stringColumn(name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// rawToString renders a JSON cell as text, unwrapping JSON strings and dropping nulls.
func rawToString(raw json.RawMessage) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	return trimmed
}

// ParseADXOperationID extracts the OperationId returned by `.export async`.
func ParseADXOperationID(body []byte) (string, error) {
	resp, err := parseKustoV1Response(body)
	if err != nil {
		return "", err
	}
	table := resp.primaryTable()
	if id := table.firstString("OperationId"); id != "" {
		return id, nil
	}
	// Some cluster versions return a single unnamed column.
	if len(table.Rows) > 0 && len(table.Rows[0]) > 0 {
		if id := rawToString(table.Rows[0][0]); id != "" {
			return id, nil
		}
	}
	return "", fmt.Errorf("export command returned no operation id")
}

// ParseADXOperationStatus extracts the state and status text from `.show operations`.
//
// A single operation id can report more than one row (Kusto retries an export on a
// different node without minting a new id). An unfinished row therefore wins over a
// terminal one, and otherwise the last row — the most recent attempt — is reported.
func ParseADXOperationStatus(body []byte) (state, status string, err error) {
	resp, parseErr := parseKustoV1Response(body)
	if parseErr != nil {
		return "", "", parseErr
	}
	table := resp.primaryTable()
	if table == nil || len(table.Rows) == 0 {
		return "", "", fmt.Errorf("operation not found")
	}
	stateIdx := table.columnIndex("State")
	if stateIdx < 0 {
		return "", "", fmt.Errorf("operation status has no State column")
	}
	statusIdx := table.columnIndex("Status")

	for _, row := range table.Rows {
		rowState := cellAt(row, stateIdx)
		switch MapADXOperationState(rowState) {
		case QueryStateRunning, QueryStateQueued:
			return rowState, cellAt(row, statusIdx), nil
		}
	}
	last := table.Rows[len(table.Rows)-1]
	return cellAt(last, stateIdx), cellAt(last, statusIdx), nil
}

// cellAt reads one cell, tolerating short rows and absent columns.
func cellAt(row []json.RawMessage, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return rawToString(row[idx])
}

// ParseADXExportedPaths extracts the blob URLs written by an export from
// `.show operation <id> details`.
func ParseADXExportedPaths(body []byte) ([]string, error) {
	resp, err := parseKustoV1Response(body)
	if err != nil {
		return nil, err
	}
	return resp.primaryTable().stringColumn("Path"), nil
}

// ParseADXSchemaColumns extracts column names from a `| getschema` query result.
func ParseADXSchemaColumns(body []byte) ([]string, error) {
	resp, err := parseKustoV1Response(body)
	if err != nil {
		return nil, err
	}
	return resp.primaryTable().stringColumn("ColumnName"), nil
}

// ParseADXError extracts a human-readable message from a Kusto error response body.
func ParseADXError(body []byte) string {
	var envelope struct {
		Error struct {
			Message    string `json:"message"`
			Code       string `json:"code"`
			InnerError struct {
				Message string `json:"message"`
			} `json:"@innererror"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil {
		if envelope.Error.InnerError.Message != "" {
			return envelope.Error.InnerError.Message
		}
		if envelope.Error.Message != "" {
			return envelope.Error.Message
		}
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 512 {
		msg = msg[:512]
	}
	return msg
}
