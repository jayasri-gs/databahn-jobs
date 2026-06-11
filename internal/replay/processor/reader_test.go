package processor

import (
	"strings"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
)

const (
	testParsedLine        = `{"rawevent":"May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu"}`
	testParsedExtracted   = "May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu"
	testRawLine           = `{"rawevent":"should-not-be-extracted"}`
	testParquetLine       = "already-extracted-parquet-row"
	testUndeliveredLine   = `{"eventtime":123,"headers":{"db_edge_ts":"123"},"body":"stored event payload"}`
	// OutOfBox / Custom nested rawevent — stored envelope on global destination
	testOutOfBoxNestedRaweventLine = `{"rawevent":{"parser_name":"on_error_decision","rawevent":"unparsed syslog line"}}`
	testOutOfBoxNestedRaweventWant = "unparsed syslog line"
	// OutOfBox new format — full stored envelope from S3 / Azure
	testOutOfBoxNewStoredLine = `{"error":"function call error for \"parse_json\"","rawevent":{"parser_name":"on_error_decision","metadata":{"db_log_type":"json"},"rawevent":"Jun 09 12:15:01 prod-web-01 sudo[8921]: session opened for user deploy(uid=1001)"}}`
	testOutOfBoxNewRawLogWant  = "Jun 09 12:15:01 prod-web-01 sudo[8921]: session opened for user deploy(uid=1001)"
)

func replayReq(replayType, forwardDataType string) model.Message {
	req := model.Message{
		ReplayType:       replayType,
		AdditionalConfig: map[string]string{},
	}
	if forwardDataType != "" {
		req.AdditionalConfig["forward_data_type"] = forwardDataType
	}
	return req
}

func assertPrepareReplayLine(t *testing.T, line string, req model.Message, isParquet bool, want string) {
	t.Helper()
	got, err := prepareReplayLine(line, req, isParquet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func assertPrepareReplayLineError(t *testing.T, line string, req model.Message, isParquet bool, wantErrSubstr string) {
	t.Helper()
	_, err := prepareReplayLine(line, req, isParquet)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if wantErrSubstr != "" && !strings.Contains(err.Error(), wantErrSubstr) {
		t.Fatalf("error %q does not contain %q", err.Error(), wantErrSubstr)
	}
}

func assertExtractSuccess(t *testing.T, extract func(string) (string, error), line, want string) {
	t.Helper()
	got, err := extract(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func assertExtractError(t *testing.T, extract func(string) (string, error), line, wantErrSubstr string) {
	t.Helper()
	_, err := extract(line)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if wantErrSubstr != "" && !strings.Contains(err.Error(), wantErrSubstr) {
		t.Fatalf("error %q does not contain %q", err.Error(), wantErrSubstr)
	}
}

// --- prepareReplayLine: UNDELIVERED ---

func TestPrepareReplayLine_UndeliveredPublishesStoredEventAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testUndeliveredLine, replayReq("UNDELIVERED", ""), false, testUndeliveredLine)
}

func TestPrepareReplayLine_UndeliveredIsCaseInsensitive(t *testing.T) {
	assertPrepareReplayLine(t, testUndeliveredLine, replayReq("undelivered", ""), false, testUndeliveredLine)
}

func TestPrepareReplayLine_UndeliveredIgnoresForwardDataTypeParsed(t *testing.T) {
	assertPrepareReplayLine(t, testParsedLine, replayReq("UNDELIVERED", "parsed"), false, testParsedLine)
}

func TestPrepareReplayLine_UndeliveredIgnoresParquetFlag(t *testing.T) {
	assertPrepareReplayLine(t, testParquetLine, replayReq("UNDELIVERED", ""), true, testParquetLine)
}

// --- prepareReplayLine: UNPARSED ---

func TestPrepareReplayLine_Unparsed_OutOfBox_nested_rawevent_string(t *testing.T) {
	assertPrepareReplayLine(t, testOutOfBoxNestedRaweventLine, replayReq("UNPARSED", ""), false, testOutOfBoxNestedRaweventWant)
}

func TestPrepareReplayLine_Unparsed_replay_type_case_insensitive(t *testing.T) {
	assertPrepareReplayLine(t, testOutOfBoxNestedRaweventLine, replayReq("unparsed", ""), false, testOutOfBoxNestedRaweventWant)
}

func TestPrepareReplayLine_Unparsed_Schemaless_envelope_missing_msg_and_rawevent(t *testing.T) {
	assertPrepareReplayLineError(t, `{"error":"parse failed"}`, replayReq("UNPARSED", ""), false, "rawevent or msg is missing or empty")
}

func TestPrepareReplayLine_Unparsed_ignores_forward_data_type(t *testing.T) {
	assertPrepareReplayLine(t, testOutOfBoxNestedRaweventLine, replayReq("UNPARSED", "raw"), false, testOutOfBoxNestedRaweventWant)
}

// --- prepareReplayLine: CUSTOM (non-parquet) ---

func TestPrepareReplayLine_CustomRawPublishesAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testRawLine, replayReq("CUSTOM", "raw"), false, testRawLine)
}

func TestPrepareReplayLine_CustomRawIsCaseInsensitive(t *testing.T) {
	assertPrepareReplayLine(t, testRawLine, replayReq("custom", "RAW"), false, testRawLine)
}

func TestPrepareReplayLine_CustomParsedExtractsStringRawevent(t *testing.T) {
	assertPrepareReplayLine(t, testParsedLine, replayReq("CUSTOM", "parsed"), false, testParsedExtracted)
}

func TestPrepareReplayLine_CustomParsedIsCaseInsensitive(t *testing.T) {
	assertPrepareReplayLine(t, testParsedLine, replayReq("CUSTOM", "PARSED"), false, testParsedExtracted)
}

func TestPrepareReplayLine_CustomMissingForwardDataTypePublishesAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testRawLine, replayReq("CUSTOM", ""), false, testRawLine)
}

func TestPrepareReplayLine_CustomUnknownForwardDataTypePublishesAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testRawLine, replayReq("CUSTOM", "something-else"), false, testRawLine)
}

func TestPrepareReplayLine_CustomParsedPropagatesExtractionError(t *testing.T) {
	assertPrepareReplayLineError(t, `{"error":"parse failed"}`, replayReq("CUSTOM", "parsed"), false, "rawevent is missing or empty")
}

// --- prepareReplayLine: CUSTOM (parquet) ---

func TestPrepareReplayLine_CustomParquetPublishesAsIsEvenWhenParsed(t *testing.T) {
	assertPrepareReplayLine(t, testParquetLine, replayReq("CUSTOM", "parsed"), true, testParquetLine)
}

func TestPrepareReplayLine_CustomParquetPublishesAsIsForRaw(t *testing.T) {
	assertPrepareReplayLine(t, testParquetLine, replayReq("CUSTOM", "raw"), true, testParquetLine)
}

// --- prepareReplayLine: empty replay type (treated as CUSTOM) ---

func TestPrepareReplayLine_EmptyReplayTypeRawPublishesAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testRawLine, replayReq("", "raw"), false, testRawLine)
}

func TestPrepareReplayLine_WhitespaceReplayTypeParsedExtractsRawevent(t *testing.T) {
	assertPrepareReplayLine(t, testParsedLine, replayReq("   ", "parsed"), false, testParsedExtracted)
}

func TestPrepareReplayLine_EmptyReplayTypeParquetPublishesAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testParquetLine, replayReq("", ""), true, testParquetLine)
}

// --- prepareReplayLine: unknown replay type (default branch) ---

func TestPrepareReplayLine_UnknownReplayTypeRawPublishesAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testRawLine, replayReq("UNKNOWN", "raw"), false, testRawLine)
}

func TestPrepareReplayLine_UnknownReplayTypeParsedExtractsRawevent(t *testing.T) {
	assertPrepareReplayLine(t, testParsedLine, replayReq("UNKNOWN", "parsed"), false, testParsedExtracted)
}

func TestPrepareReplayLine_UnknownReplayTypeMissingForwardDataTypePublishesAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testRawLine, replayReq("UNKNOWN", ""), false, testRawLine)
}

func TestPrepareReplayLine_UnknownReplayTypeParquetPublishesAsIs(t *testing.T) {
	assertPrepareReplayLine(t, testParquetLine, replayReq("UNKNOWN", "parsed"), true, testParquetLine)
}

func TestPrepareReplayLine_UnknownReplayTypeParsedPropagatesExtractionError(t *testing.T) {
	assertPrepareReplayLineError(t, "not-json", replayReq("UNKNOWN", "parsed"), false, "failed to unmarshal Parsed event to extract rawevent")
}

func TestGetRawDataFromUnparsedObject_OutOfBox_new_delegates_to_model(t *testing.T) {
	assertExtractSuccess(t, getRawDataFromUnparsedObject, testOutOfBoxNewStoredLine, testOutOfBoxNewRawLogWant)
}

// All normalization scenarios (Custom, OutOfBox old/new, Schemaless, errors):
// see internal/replay/model/unparsed_event_test.go

// --- getRawDataFromDataBahnParsedObject ---

func TestGetRawDataFromDataBahnParsedObject_StringRawevent(t *testing.T) {
	assertExtractSuccess(t, getRawDataFromDataBahnParsedObject, testParsedLine, testParsedExtracted)
}

func TestGetRawDataFromDataBahnParsedObject_ObjectRaweventWithMsg(t *testing.T) {
	line := `{"error":"function call error","rawevent":{"metadata":{"db_log_type":"json"},"msg":"May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu","parser_name":"on_error_decision"}}`
	assertExtractSuccess(t, getRawDataFromDataBahnParsedObject, line, testParsedExtracted)
}

func TestGetRawDataFromDataBahnParsedObject_InvalidJSON(t *testing.T) {
	assertExtractError(t, getRawDataFromDataBahnParsedObject, "not-json", "failed to unmarshal Parsed event to extract rawevent")
}

func TestGetRawDataFromDataBahnParsedObject_MissingRaweventField(t *testing.T) {
	assertExtractError(t, getRawDataFromDataBahnParsedObject, `{"error":"parse failed"}`, "rawevent is missing or empty")
}

func TestGetRawDataFromDataBahnParsedObject_NullRawevent(t *testing.T) {
	assertExtractError(t, getRawDataFromDataBahnParsedObject, `{"rawevent":null}`, "rawevent is missing or empty")
}

func TestGetRawDataFromDataBahnParsedObject_EmptyStringRawevent(t *testing.T) {
	assertExtractError(t, getRawDataFromDataBahnParsedObject, `{"rawevent":""}`, "rawevent is missing or empty")
}

func TestGetRawDataFromDataBahnParsedObject_ObjectRaweventWithoutMsg(t *testing.T) {
	assertExtractError(t, getRawDataFromDataBahnParsedObject, `{"rawevent":{"metadata":{"db_log_type":"json"}}}`, "rawevent must be a string or an object with msg")
}

func TestGetRawDataFromDataBahnParsedObject_ObjectRaweventWithEmptyMsg(t *testing.T) {
	assertExtractError(t, getRawDataFromDataBahnParsedObject, `{"rawevent":{"msg":""}}`, "rawevent must be a string or an object with msg")
}

func TestGetRawDataFromDataBahnParsedObject_RaweventIsNonStringNonObjectWithMsg(t *testing.T) {
	assertExtractError(t, getRawDataFromDataBahnParsedObject, `{"rawevent":123}`, "rawevent must be a string or an object with msg")
}
