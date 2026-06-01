package processor

import (
	"strings"
	"testing"
)

func TestGetRawDataFromDataBahnParsedObject_StringRawEvent(t *testing.T) {
	line := `{"rawevent":"May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu"}`
	got, err := getRawDataFromDataBahnParsedObject(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestGetRawDataFromDataBahnParsedObject_ObjectRawEventWithMsg(t *testing.T) {
	line := `{"error":"function call error for \"parse_json\"","rawevent":{"error":"function call error","metadata":{"db_device_type":"entra_id","db_log_type":"json"},"msg":"May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu from 192.168.1.25 port 54321 ssh2","parser_name":"on_error_decision"}}`
	got, err := getRawDataFromDataBahnParsedObject(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu from 192.168.1.25 port 54321 ssh2"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestGetRawDataFromDataBahnParsedObject_MissingRawEvent(t *testing.T) {
	_, err := getRawDataFromDataBahnParsedObject(`{"error":"parse failed"}`)
	if err == nil {
		t.Fatal("expected error for missing rawevent")
	}
	if !strings.Contains(err.Error(), "rawevent is missing or empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetRawDataFromDataBahnParsedObject_ObjectWithoutMsg(t *testing.T) {
	_, err := getRawDataFromDataBahnParsedObject(`{"rawevent":{"metadata":{"db_log_type":"json"}}}`)
	if err == nil {
		t.Fatal("expected error for object rawevent without msg")
	}
	if !strings.Contains(err.Error(), "rawevent must be a string or an object with msg") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetRawDataFromDataBahnParsedObject_EmptyStringRawEvent(t *testing.T) {
	_, err := getRawDataFromDataBahnParsedObject(`{"rawevent":""}`)
	if err == nil {
		t.Fatal("expected error for empty string rawevent")
	}
	if !strings.Contains(err.Error(), "rawevent is missing or empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetRawDataFromDataBahnParsedObject_NullRawEvent(t *testing.T) {
	_, err := getRawDataFromDataBahnParsedObject(`{"rawevent":null}`)
	if err == nil {
		t.Fatal("expected error for null rawevent")
	}
	if !strings.Contains(err.Error(), "rawevent is missing or empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetRawDataFromDataBahnParsedObject_InvalidJSON(t *testing.T) {
	_, err := getRawDataFromDataBahnParsedObject(`not-json`)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "failed to unmarshal Parsed event to extract rawevent") {
		t.Fatalf("unexpected error: %v", err)
	}
}
