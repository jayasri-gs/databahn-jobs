package processor

import (
	"os"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
)

func TestReplayDataBrokers(t *testing.T) {
	t.Setenv(constants.KafkaInputBootstrapServers, "input:9092")
	t.Setenv(constants.KafkaProcessingBootstrapServers, "processing:9092")

	if got := replayDataBrokers("CUSTOM"); got != "input:9092" {
		t.Fatalf("CUSTOM: got %q, want input:9092", got)
	}
	if got := replayDataBrokers("custom"); got != "input:9092" {
		t.Fatalf("custom: got %q, want input:9092", got)
	}
	if !replayUsesInputKafka(" CUSTOM ") {
		t.Fatal("replayUsesInputKafka should be case-insensitive")
	}
	if got := replayDataBrokers("UNDELIVERED"); got != "processing:9092" {
		t.Fatalf("UNDELIVERED: got %q, want processing:9092", got)
	}
	if got := replayDataBrokers("undelivered"); got != "processing:9092" {
		t.Fatalf("undelivered: got %q, want processing:9092", got)
	}
	if !matchReplayType("Undelivered", "UNDELIVERED") || !matchReplayType("custom", "CUSTOM") {
		t.Fatal("matchReplayType should be case-insensitive")
	}
	if got := replayDataBrokers("undelivered"); got != "processing:9092" {
		t.Fatalf("undelivered: got %q, want processing:9092", got)
	}
	if got := replayDataBrokers("UNPARSED"); got != "processing:9092" {
		t.Fatalf("UNPARSED: got %q, want processing:9092", got)
	}
	if got := replayDataBrokers(""); got != "input:9092" {
		t.Fatalf("empty: got %q, want input:9092", got)
	}
	if got := replayDataBrokers("   "); got != "input:9092" {
		t.Fatalf("whitespace: got %q, want input:9092", got)
	}
}

func TestReplayDataBrokersUnsetEnv(t *testing.T) {
	os.Unsetenv(constants.KafkaInputBootstrapServers)
	os.Unsetenv(constants.KafkaProcessingBootstrapServers)
	if got := replayDataBrokers("CUSTOM"); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}
