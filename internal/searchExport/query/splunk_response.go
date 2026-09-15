package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const (
	errParseSplunkResponse = "parse Splunk export response: %w"
	errParseSplunkFrame    = "parse Splunk export frame: %w"
	maxSplunkMessageText   = 512
)

// SplunkResponseMessage is one Splunk REST message frame entry.
type SplunkResponseMessage struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// SplunkStreamMetadata captures decoder observations for truncation reconciliation in a
// later step. It is populated by decodeSplunkJsonRowsResponseWithMetadata and discarded by
// decodeSplunkJsonRowsResponse until the executor wires reconciliation.
type SplunkStreamMetadata struct {
	Messages             []SplunkResponseMessage
	DroppedFieldNames    []string
	TruncationHints      []string
	PreviewFramesSkipped int64
}

type splunkDecodeState struct {
	header           []string
	headerSet        map[string]struct{}
	onColumns        func([]string) error
	onRow            func([]interface{}) error
	onColumnsCalled  bool
	rows             int64
	metadata         SplunkStreamMetadata
	warnedDropFields bool
}

// decodeSplunkJsonRowsResponse streams a Splunk /export json_rows body, invoking onColumns
// once before the first emitted row and onRow for every non-preview result row.
func decodeSplunkJsonRowsResponse(r io.Reader, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	rows, _, err := decodeSplunkJsonRowsResponseWithMetadata(r, onColumns, onRow)
	return rows, err
}

// decodeSplunkJsonRowsResponseWithMetadata is the decoder entry point that also returns
// observations used by truncation reconciliation in a later step.
func decodeSplunkJsonRowsResponseWithMetadata(
	r io.Reader,
	onColumns func([]string) error,
	onRow func([]interface{}) error,
) (int64, SplunkStreamMetadata, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()

	st := &splunkDecodeState{
		onColumns: onColumns,
		onRow:     onRow,
		headerSet: make(map[string]struct{}),
	}

	for {
		var frame json.RawMessage
		err := dec.Decode(&frame)
		if err == io.EOF {
			break
		}
		if err != nil {
			return st.rows, st.metadata, fmt.Errorf(errParseSplunkResponse, err)
		}
		if err := decodeSplunkJsonRowsFrame(frame, st); err != nil {
			return st.rows, st.metadata, err
		}
	}

	return st.rows, st.metadata, nil
}

func decodeSplunkJsonRowsFrame(raw json.RawMessage, st *splunkDecodeState) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	if err := expectDelim(dec, '{'); err != nil {
		return fmt.Errorf(errParseSplunkFrame, err)
	}

	var preview *bool
	var fields []string
	var pendingRows [][]json.RawMessage
	var frameMessages []SplunkResponseMessage
	var frameError string

	for dec.More() {
		key, err := objectKey(dec)
		if err != nil {
			return fmt.Errorf(errParseSplunkFrame, err)
		}
		switch key {
		case "preview":
			var value bool
			if err := dec.Decode(&value); err != nil {
				return fmt.Errorf("parse Splunk preview flag: %w", err)
			}
			preview = &value
		case "fields":
			if err := dec.Decode(&fields); err != nil {
				return fmt.Errorf("parse Splunk fields: %w", err)
			}
		case "rows":
			rows, err := decodeSplunkRawRows(dec)
			if err != nil {
				return err
			}
			pendingRows = append(pendingRows, rows...)
		case "messages":
			if err := dec.Decode(&frameMessages); err != nil {
				return fmt.Errorf("parse Splunk messages: %w", err)
			}
		case "error":
			frameError, err = decodeSplunkErrorValue(dec)
			if err != nil {
				return err
			}
		default:
			if err := skipValue(dec); err != nil {
				return fmt.Errorf(errParseSplunkFrame, err)
			}
		}
	}

	if err := expectDelim(dec, '}'); err != nil {
		return fmt.Errorf(errParseSplunkFrame, err)
	}

	if frameError != "" {
		return fmt.Errorf("splunk export failed: %s", frameError)
	}

	st.recordMessages(frameMessages)

	if preview != nil && *preview {
		st.metadata.PreviewFramesSkipped++
		return nil
	}

	if len(fields) > 0 {
		if err := st.bindHeader(fields); err != nil {
			return err
		}
	}

	frameFields := fields
	if len(frameFields) == 0 {
		frameFields = st.header
	}

	for _, rawRow := range pendingRows {
		row, err := st.projectRow(frameFields, rawRow)
		if err != nil {
			return err
		}
		if err := st.onRow(row); err != nil {
			return err
		}
		st.rows++
	}

	return nil
}

func decodeSplunkRawRows(dec *json.Decoder) ([][]json.RawMessage, error) {
	if err := expectDelim(dec, '['); err != nil {
		return nil, fmt.Errorf("parse Splunk rows: %w", err)
	}
	var rows [][]json.RawMessage
	for dec.More() {
		var rawRow []json.RawMessage
		if err := dec.Decode(&rawRow); err != nil {
			return nil, fmt.Errorf("parse Splunk row: %w", err)
		}
		rows = append(rows, rawRow)
	}
	if err := expectDelim(dec, ']'); err != nil {
		return nil, fmt.Errorf("parse Splunk rows: %w", err)
	}
	return rows, nil
}

func decodeSplunkErrorValue(dec *json.Decoder) (string, error) {
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return "", fmt.Errorf("parse Splunk error value: %w", err)
	}
	return boundSplunkText(string(raw)), nil
}

func (st *splunkDecodeState) bindHeader(fields []string) error {
	if len(st.header) == 0 {
		st.header = append([]string(nil), fields...)
		for _, name := range st.header {
			st.headerSet[name] = struct{}{}
		}
		if st.onColumns != nil && !st.onColumnsCalled {
			if err := st.onColumns(st.header); err != nil {
				return err
			}
			st.onColumnsCalled = true
		}
		return nil
	}
	if !sameSplunkColumns(st.header, fields) {
		// Later frames may reorder fields; projection uses names, not positions.
		return nil
	}
	return nil
}

func (st *splunkDecodeState) projectRow(frameFields []string, rawRow []json.RawMessage) ([]interface{}, error) {
	if len(st.header) == 0 {
		return nil, fmt.Errorf("Splunk export response contained rows before a column header")
	}

	indexByField := make(map[string]int, len(frameFields))
	for i, name := range frameFields {
		indexByField[name] = i
	}

	var dropped []string
	for _, name := range frameFields {
		if _, ok := st.headerSet[name]; !ok {
			dropped = append(dropped, name)
		}
	}
	if len(dropped) > 0 {
		st.noteDroppedFields(dropped)
	}

	row := make([]interface{}, len(st.header))
	for i, col := range st.header {
		idx, ok := indexByField[col]
		if !ok || idx >= len(rawRow) {
			row[i] = nil
			continue
		}
		row[i] = rawToExportValue(rawRow[idx])
	}
	return row, nil
}

func (st *splunkDecodeState) noteDroppedFields(names []string) {
	unique := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, ok := st.headerSet[name]; ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		unique = append(unique, name)
		st.metadata.DroppedFieldNames = appendUniqueString(st.metadata.DroppedFieldNames, name)
	}
	if len(unique) == 0 || st.warnedDropFields {
		return
	}
	st.warnedDropFields = true
	logging.GetLogger().Warn("Splunk export dropped fields absent from the export header",
		zap.Strings("fields", unique),
		zap.Int("count", len(unique)))
}

func (st *splunkDecodeState) recordMessages(msgs []SplunkResponseMessage) {
	if len(msgs) == 0 {
		return
	}
	for _, msg := range msgs {
		msg.Type = strings.TrimSpace(msg.Type)
		msg.Text = boundSplunkText(msg.Text)
		if msg.Type == "" && msg.Text == "" {
			continue
		}
		st.metadata.Messages = append(st.metadata.Messages, msg)
		st.logSplunkMessage(msg)
		if hint := splunkTruncationHint(msg); hint != "" {
			st.metadata.TruncationHints = appendUniqueString(st.metadata.TruncationHints, hint)
		}
	}
}

func (st *splunkDecodeState) logSplunkMessage(msg SplunkResponseMessage) {
	log := logging.GetLogger()
	fields := []zap.Field{zap.String("type", msg.Type), zap.String("text", msg.Text)}
	switch strings.ToUpper(msg.Type) {
	case "ERROR", "FATAL":
		log.Error("Splunk export message", fields...)
	case "WARN", "WARNING":
		log.Warn("Splunk export message", fields...)
	default:
		log.Info("Splunk export message", fields...)
	}
}

func splunkTruncationHint(msg SplunkResponseMessage) string {
	text := strings.ToLower(msg.Text)
	switch {
	case strings.Contains(text, "truncated"),
		strings.Contains(text, "maxresultrows"),
		strings.Contains(text, "limit"):
		return boundSplunkText(msg.Text)
	default:
		return ""
	}
}

func boundSplunkText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	if len(text) > maxSplunkMessageText {
		return text[:maxSplunkMessageText] + "…"
	}
	return text
}

func appendUniqueString(list []string, value string) []string {
	for _, existing := range list {
		if existing == value {
			return list
		}
	}
	return append(list, value)
}
