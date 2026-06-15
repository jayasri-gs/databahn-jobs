package model

import (
	"strings"
	"testing"
)

const (
	wantOutOfBoxNewRawLog = "Jun 09 12:15:01 prod-web-01 sudo[8921]: session opened for user deploy(uid=1001)"
	wantOutOfBoxOldRawLog = "Jun 03 21:12:45 server01 kernel: device eth0 entered promiscuous mode"
	wantCustomRawLog      = "hello world this is not syslog format at all"
	wantSchemalessRawLog  = "Jun 09 12:15:01 prod-web-01 docker[1234]: Authorization: Bearer ghp_1234567890abcdefghijklmnopqrstuvwxyz"
)

type unparsedExtractScenario struct {
	name           string
	normalization  string
	storedOn       string
	line           string
	want           string
	wantErrContain string
}

func TestExtractUnparsedRawLog(t *testing.T) {
	scenarios := []unparsedExtractScenario{
		{
			name:          "OutOfBox_new_global_destination_envelope",
			normalization: "OutOfBox (normalization-service)",
			storedOn:      "S3 / Azure — inner envelope with rawevent.rawevent",
			line:          `{"error":"function call error for \"parse_json\"","rawevent":{"parser_name":"on_error_decision","metadata":{"db_log_type":"json"},"rawevent":"Jun 09 12:15:01 prod-web-01 sudo[8921]: session opened for user deploy(uid=1001)"}}`,
			want:          wantOutOfBoxNewRawLog,
		},
		{
			name:          "OutOfBox_old_global_destination_envelope",
			normalization: "OutOfBox (normalization-service, legacy format)",
			storedOn:      "S3 / Azure — inner envelope with rawevent.msg",
			line:          `{"error":"function call error for \"parse_json\" at (15:35): unable to parse json: expected value at line 1 column 1","rawevent":{"error":"function call error for \"parse_json\" at (15:35): unable to parse json: expected value at line 1 column 1","metadata":{"db_device_type":"entra_id","db_device_vendor":"microsoft","db_event_source_id":"c2e033f1-75d1-4caa-96c1-75b86efd0e85","db_log_type":"json"},"msg":"Jun 03 21:12:45 server01 kernel: device eth0 entered promiscuous mode","parser_name":"on_error_decision"}}`,
			want:          wantOutOfBoxOldRawLog,
		},
		{
			name:          "Custom_normalization_global_destination_envelope",
			normalization: "Custom (custom-normalization-service)",
			storedOn:      "S3 / Azure — inner envelope with rawevent.msg",
			line:          `{"error":"unable to parse","rawevent":{"error":"unable to parse","metadata":{"db_device_type":"custom_test-unparsed","db_device_vendor":"test-unparsed","db_event_source_id":"aa3d0543-97f0-422d-8106-eabdc63244ea","db_log_type":"json"},"msg":"hello world this is not syslog format at all","parser_name":"on_error_decision"}}`,
			want:          wantCustomRawLog,
		},
		{
			name:          "Schemaless_normalization_flat_json",
			normalization: "Schemaless (schemaless-normalization-service)",
			storedOn:      "S3 / Azure — flat JSON with top-level msg (no rawevent wrapper)",
			line:          `{"error":"Failed to parse data: ReadMapCB: expect { or n, but found J","metadata":{"db_device_type":"test-unparsed","db_device_vendor":"test-unparsed","db_event_source_id":"c55d431a-dd9b-4137-9eab-008d34144bee","db_log_type":"json"},"msg":"Jun 09 12:15:01 prod-web-01 docker[1234]: Authorization: Bearer ghp_1234567890abcdefghijklmnopqrstuvwxyz","parser_name":"schemaless"}`,
			want:          wantSchemalessRawLog,
		},
		{
			name:           "error_top_level_rawevent_string_not_global_unparsed_shape",
			normalization:  "n/a (CUSTOM parsed backup shape)",
			storedOn:       "top-level rawevent as plain string — not produced by global unparsed dispensers",
			line:           `{"rawevent":"` + wantOutOfBoxNewRawLog + `"}`,
			wantErrContain: "failed to unmarshal Unparsed event to extract rawevent",
		},
		{
			name:          "Schemaless_msg_may_contain_logstash_literal_substring",
			normalization: "Schemaless",
			storedOn:      "flat JSON — msg field contains %{[event][message]} as text, not whole line",
			line:          `{"error":"parse failed","metadata":{"db_log_type":"json"},"msg":"field ref %{[event][message]} in log","parser_name":"schemaless"}`,
			want:          "field ref %{[event][message]} in log",
		},
		{
			name:           "error_invalid_json",
			normalization:  "n/a",
			storedOn:       "corrupt line",
			line:           "not-json",
			wantErrContain: "failed to unmarshal Unparsed event to extract rawevent",
		},
		{
			name:           "error_vector_envelope_missing_msg_and_rawevent",
			normalization:  "Custom / OutOfBox",
			storedOn:       "inner rawevent object without msg or rawevent field",
			line:           `{"error":"parse failed"}`,
			wantErrContain: "rawevent or msg is missing or empty",
		},
		{
			name:           "error_null_top_level_rawevent",
			normalization:  "any",
			storedOn:       "rawevent: null",
			line:           `{"rawevent":null}`,
			wantErrContain: "rawevent or msg is missing or empty",
		},
		{
			name:           "error_top_level_rawevent_string",
			normalization:  "n/a",
			storedOn:       "rawevent: \"\" — string type, not object",
			line:           `{"rawevent":""}`,
			wantErrContain: "failed to unmarshal Unparsed event to extract rawevent",
		},
		{
			name:           "error_custom_nested_rawevent_missing_payload",
			normalization:  "Custom / OutOfBox",
			storedOn:       "rawevent object with metadata only",
			line:           `{"rawevent":{"parser_name":"on_error_decision","metadata":{"db_log_type":"json"}}}`,
			wantErrContain: "nested msg or rawevent is missing or empty",
		},
		{
			name:           "error_custom_nested_rawevent_empty_payload",
			normalization:  "Custom / OutOfBox",
			storedOn:       "rawevent.msg and rawevent.rawevent both empty",
			line:           `{"rawevent":{"rawevent":"","msg":""}}`,
			wantErrContain: "nested msg or rawevent is missing or empty",
		},
		{
			name:           "error_rawevent_wrong_type",
			normalization:  "any",
			storedOn:       "rawevent is number not string/object",
			line:           `{"rawevent":123}`,
			wantErrContain: "failed to unmarshal Unparsed event to extract rawevent",
		},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			t.Logf("normalization: %s", sc.normalization)
			t.Logf("stored on global destination: %s", sc.storedOn)

			got, err := ExtractUnparsedRawLog(sc.line)
			if sc.wantErrContain != "" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), sc.wantErrContain) {
					t.Fatalf("error %q does not contain %q", err.Error(), sc.wantErrContain)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != sc.want {
				t.Fatalf("got %q, want %q", got, sc.want)
			}
		})
	}
}
