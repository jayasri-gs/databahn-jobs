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
	RawEvent *UnparsedNestedPayload `json:"rawevent"`
	Message  string                 `json:"msg"`
}

// UnparsedNestedPayload is the inner object under rawevent for Custom and OutOfBox.
type UnparsedNestedPayload struct {
	Msg      string `json:"msg"`
	RawEvent string `json:"rawevent"`
}

// ExtractUnparsedRawLog returns the original raw log from a stored unparsed JSON line.
//
// Supported stored formats (global unparsed destination only):
//  1. Schemaless — flat { error, metadata, msg, parser_name } (no top-level rawevent)
//  2. Custom / OutOfBox old — { error, rawevent: { msg, ... } }
//  3. OutOfBox new — { error, rawevent: { rawevent, ... } }
//
// Top-level rawevent as a plain string is not produced by global unparsed dispensers;
// that shape is handled by CUSTOM parsed replay (getRawDataFromDataBahnParsedObject).
func ExtractUnparsedRawLog(line string) (string, error) {
	trimmed := strings.TrimSpace(line)
	// Reject blank lines before JSON parse.
	if trimmed == "" {
		return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: line is empty")
	}

	var stored UnparsedStoredLine
	if err := json.Unmarshal([]byte(trimmed), &stored); err != nil {
		return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: %v", err)
	}

	// Schemaless: flat JSON has top-level msg and no rawevent object (or rawevent is null).
	if stored.RawEvent == nil {
		// Envelope must expose the raw log via top-level msg when rawevent is absent.
		if stored.Message == "" {
			return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: rawevent or msg is missing or empty")
		}
		return stored.Message, nil
	}

	// Custom / OutOfBox old: nested raw log in rawevent.msg.
	if stored.RawEvent.Msg != "" {
		return stored.RawEvent.Msg, nil
	}
	// OutOfBox new: nested raw log in rawevent.rawevent (checked after msg for backward compatibility).
	if stored.RawEvent.RawEvent != "" {
		return stored.RawEvent.RawEvent, nil
	}
	// rawevent object present but neither nested field holds the raw log.
	return "", fmt.Errorf("failed to unmarshal Unparsed event to extract rawevent: nested msg or rawevent is missing or empty")
}
