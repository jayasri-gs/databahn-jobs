package acknowledgement

import (
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
)

func TestGetQueryBatchSize(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv(ackProcessorQueryBatchSizeEnv, "")
		if got := getQueryBatchSize(); got != constants.QueryBatchSize {
			t.Fatalf("expected %d, got %d", constants.QueryBatchSize, got)
		}
	})

	t.Run("custom", func(t *testing.T) {
		t.Setenv(ackProcessorQueryBatchSizeEnv, "25")
		if got := getQueryBatchSize(); got != 25 {
			t.Fatalf("expected 25, got %d", got)
		}
	})

	t.Run("invalid falls back to default", func(t *testing.T) {
		t.Setenv(ackProcessorQueryBatchSizeEnv, "0")
		if got := getQueryBatchSize(); got != constants.QueryBatchSize {
			t.Fatalf("expected %d, got %d", constants.QueryBatchSize, got)
		}
	})
}

func TestGetEntityPageSize(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv(ackProcessorEntityPageSizeEnv, "")
		if got := getEntityPageSize(); got != defaultEntityPageSize {
			t.Fatalf("expected %d, got %d", defaultEntityPageSize, got)
		}
	})

	t.Run("custom", func(t *testing.T) {
		t.Setenv(ackProcessorEntityPageSizeEnv, "150")
		if got := getEntityPageSize(); got != 150 {
			t.Fatalf("expected 150, got %d", got)
		}
	})
}

func TestGetEntityUpdateBatchSize(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv(ackProcessorEntityUpdateBatchSizeEnv, "")
		if got := getEntityUpdateBatchSize(); got != defaultEntityUpdateBatchSize {
			t.Fatalf("expected %d, got %d", defaultEntityUpdateBatchSize, got)
		}
	})

	t.Run("custom", func(t *testing.T) {
		t.Setenv(ackProcessorEntityUpdateBatchSizeEnv, "40")
		if got := getEntityUpdateBatchSize(); got != 40 {
			t.Fatalf("expected 40, got %d", got)
		}
	})
}
