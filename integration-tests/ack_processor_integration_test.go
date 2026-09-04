//go:build integration

package integrationtests

import (
	"testing"

	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
	ack "github.com/databahn-ai/databahn-jobs/internal/acknowledgement"
)

func TestAckProcessorPaginatesAcrossMultiplePages(t *testing.T) {
	setupAckProcessorIntegration(t)
	fixture := seedAckProcessorPaginationFixture(t)

	result := ack.ProcessAck()
	if len(result.Errors) > 0 {
		t.Fatalf("ProcessAck returned errors: %+v", result.Errors)
	}

	if len(fixture.Entities) != 3 {
		t.Fatalf("expected 3 seeded entities, got %d", len(fixture.Entities))
	}

	for _, entity := range fixture.Entities {
		if got := getAckProcessStatus(t, entity.AckID); got != fixtures.AckProcessStatusProcessed {
			t.Fatalf("ack %s process_status = %q, want %q", entity.AckID, got, fixtures.AckProcessStatusProcessed)
		}
		if got := getLogSourceStatus(t, entity.SourceID); got != "ACTIVE" {
			t.Fatalf("log_source %s status = %q, want ACTIVE", entity.SourceID, got)
		}
	}
}

func TestAckProcessorSuppressesOlderRequestAcks(t *testing.T) {
	setupAckProcessorIntegration(t)
	fixture := seedAckProcessorSuppressionFixture(t)
	if len(fixture.Entities) != 1 {
		t.Fatalf("expected 1 seeded entity, got %d", len(fixture.Entities))
	}
	entity := fixture.Entities[0]

	result := ack.ProcessAck()
	if len(result.Errors) > 0 {
		t.Fatalf("ProcessAck returned errors: %+v", result.Errors)
	}

	if got := getAckProcessStatus(t, entity.AckID); got != fixtures.AckProcessStatusProcessed {
		t.Fatalf("latest ack %s process_status = %q, want %q", entity.AckID, got, fixtures.AckProcessStatusProcessed)
	}
	if got := getAckProcessStatus(t, entity.LegacyAck); got != fixtures.AckProcessStatusSuppressed {
		t.Fatalf("legacy ack %s process_status = %q, want %q", entity.LegacyAck, got, fixtures.AckProcessStatusSuppressed)
	}
	if got := getLogSourceStatus(t, entity.SourceID); got != "ACTIVE" {
		t.Fatalf("log_source %s status = %q, want ACTIVE", entity.SourceID, got)
	}
}

func TestAckProcessorEmptyBacklog(t *testing.T) {
	setupAckProcessorIntegration(t)

	result := ack.ProcessAck()
	if len(result.Errors) > 0 {
		t.Fatalf("ProcessAck returned errors: %+v", result.Errors)
	}
}

func TestAckProcessorFailureAckMarksEntityErrored(t *testing.T) {
	setupAckProcessorIntegration(t)
	fixture := seedAckProcessorFailureFixture(t)
	if len(fixture.Entities) != 1 {
		t.Fatalf("expected 1 seeded entity, got %d", len(fixture.Entities))
	}
	entity := fixture.Entities[0]

	result := ack.ProcessAck()
	if len(result.Errors) > 0 {
		t.Fatalf("ProcessAck returned errors: %+v", result.Errors)
	}

	if got := getAckProcessStatus(t, entity.AckID); got != fixtures.AckProcessStatusProcessed {
		t.Fatalf("ack %s process_status = %q, want %q", entity.AckID, got, fixtures.AckProcessStatusProcessed)
	}
	if got := getLogSourceStatus(t, entity.SourceID); got != "ERRORED" {
		t.Fatalf("log_source %s status = %q, want ERRORED", entity.SourceID, got)
	}
}
