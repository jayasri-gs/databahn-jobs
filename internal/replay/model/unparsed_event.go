package model

import (
	"encoding/json"
	"fmt"
	"strings"
)

// UnparsedStoredLine is the top-level shape of a line read from global unparsed
// destination (S3 / Azure / Snowflake). Supports Schemaless flat JSON and
// Custom / OutOfBox wrapped envelopes.
type UnparsedStoredLine struct {
	RawEvent json.RawMessage `json:"rawevent"`
	Message  string          `json:"msg"`
}

// UnparsedNestedPayload is the inner object under rawevent for Custom and OutOfBox.
type UnparsedNestedPayload struct {
	Msg      string `json:"msg"`
	RawEvent string `json:"rawevent"`
}

// ExtractUnparsedRawLog returns the original raw log from a stored unparsed JSON line.
//
// Supported stored formats:
//  1. Schemaless — flat { error, metadata, msg, parser_name } (no top-level rawevent)
//  2. Top-level rawevent string
//  3. Custom / OutOfBox old — { rawevent: { msg, ... } }
//  4. OutOfBox new — { rawevent: { rawevent, ... } }
func ExtractUnparsedRawLog(line string) (string, error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: line is empty")
	}
	if strings.Contains(trimmed, "%{[event][message]}") {
		return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: invalid stored line from S3 dispenser (logstash field literal)")
	}

	var stored UnparsedStoredLine
	if err := json.Unmarshal([]byte(trimmed), &stored); err != nil {
		return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: %v", err)
	}

	if len(stored.RawEvent) == 0 {
		if stored.Message == "" {
			return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: rawevent or msg is missing or empty")
		}
		return stored.Message, nil
	}

	var raweventString string
	if err := json.Unmarshal(stored.RawEvent, &raweventString); err == nil {
		if raweventString == "" {
			return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: rawevent is missing or empty")
		}
		return raweventString, nil
	}

	var nested UnparsedNestedPayload
	if err := json.Unmarshal(stored.RawEvent, &nested); err != nil {
		return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: %v", err)
	}
	if nested.Msg != "" {
		return nested.Msg, nil
	}
	if nested.RawEvent != "" {
		return nested.RawEvent, nil
	}
	return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: nested msg or rawevent is missing or empty")
}
