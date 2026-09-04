//go:build integration

package integrationtests

import (
	"context"
	"testing"

	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/google/uuid"
)

func setupAckProcessorIntegration(t *testing.T) {
	t.Helper()
	config.SetDBForTest(GormDB())
	t.Setenv("ACK_PROCESSOR_ENTITY_PAGE_SIZE", "2")
	t.Setenv("ACK_PROCESSOR_QUERY_BATCH_SIZE", "50")
	t.Setenv("ACK_PROCESSOR_ENTITY_UPDATE_BATCH_SIZE", "50")
	t.Setenv("ACK_PROCESSOR_ACK_READ_OLDER_THAN_SECONDS", "60")
	t.Cleanup(func() {
		config.SetDBForTest(nil)
	})
}

func seedAckProcessorPaginationFixture(t *testing.T) *fixtures.AckProcessorFixture {
	t.Helper()
	fixture, err := fixtures.SeedAckProcessorPaginationFixture(context.Background(), GormDB())
	if err != nil {
		t.Fatalf("seed ack pagination fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := fixtures.CleanupAckProcessorFixture(context.Background(), GormDB(), fixture); err != nil {
			t.Fatalf("cleanup ack pagination fixture: %v", err)
		}
	})
	return fixture
}

func seedAckProcessorSuppressionFixture(t *testing.T) *fixtures.AckProcessorFixture {
	t.Helper()
	fixture, err := fixtures.SeedAckProcessorSuppressionFixture(context.Background(), GormDB())
	if err != nil {
		t.Fatalf("seed ack suppression fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := fixtures.CleanupAckProcessorFixture(context.Background(), GormDB(), fixture); err != nil {
			t.Fatalf("cleanup ack suppression fixture: %v", err)
		}
	})
	return fixture
}

func seedAckProcessorFailureFixture(t *testing.T) *fixtures.AckProcessorFixture {
	t.Helper()
	fixture, err := fixtures.SeedAckProcessorFailureFixture(context.Background(), GormDB())
	if err != nil {
		t.Fatalf("seed ack failure fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := fixtures.CleanupAckProcessorFixture(context.Background(), GormDB(), fixture); err != nil {
			t.Fatalf("cleanup ack failure fixture: %v", err)
		}
	})
	return fixture
}

func getAckProcessStatus(t *testing.T, ackID uuid.UUID) string {
	t.Helper()
	var row struct {
		ProcessStatus string `gorm:"column:process_status"`
	}
	if err := GormDB().Table("change_flag_acks").Select("process_status").Where("id = ?", ackID).Take(&row).Error; err != nil {
		t.Fatalf("load ack %s: %v", ackID, err)
	}
	return row.ProcessStatus
}

func getLogSourceStatus(t *testing.T, sourceID uuid.UUID) string {
	t.Helper()
	var row struct {
		Status string `gorm:"column:status"`
	}
	if err := GormDB().Table("log_source").Select("status").Where("id = ?", sourceID).Take(&row).Error; err != nil {
		t.Fatalf("load log_source %s: %v", sourceID, err)
	}
	return row.Status
}
