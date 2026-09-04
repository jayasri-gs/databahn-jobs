package acknowledgement

import (
	"errors"
	"regexp"
	"testing"
	"time"

	ackPkg "github.com/databahn-ai/common-utils/ack"
	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/dbtest"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestParseAckLookbackDuration(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    time.Duration
		expectError bool
	}{
		{
			name:     "hours",
			input:    "5h",
			expected: 5 * time.Hour,
		},
		{
			name:     "minutes",
			input:    "30m",
			expected: 30 * time.Minute,
		},
		{
			name:     "seconds",
			input:    "3600s",
			expected: time.Hour,
		},
		{
			name:     "days",
			input:    "3d",
			expected: 72 * time.Hour,
		},
		{
			name:        "invalid",
			input:       "abc",
			expectError: true,
		},
		{
			name:        "empty",
			input:       "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := parseAckLookbackDuration(tt.input)
			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if actual != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

func TestGetAckLookbackStart(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	t.Run("unset returns nil", func(t *testing.T) {
		t.Setenv(ackLookbackDurationEnv, "")
		start, err := getAckLookbackStart(now)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if start != nil {
			t.Fatalf("expected nil start, got %v", start)
		}
	})

	t.Run("set returns start time", func(t *testing.T) {
		t.Setenv(ackLookbackDurationEnv, "5h")
		start, err := getAckLookbackStart(now)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expected := now.Add(-5 * time.Hour)
		if start == nil || !start.Equal(expected) {
			t.Fatalf("expected %v, got %v", expected, start)
		}
	})

	t.Run("zero duration returns error", func(t *testing.T) {
		t.Setenv(ackLookbackDurationEnv, "0s")
		_, err := getAckLookbackStart(now)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestUpdateStatusSuppressesUnsupportedAckOnlyEntityTypes(t *testing.T) {
	entityTypes := []string{
		"pipeline",
		"data-replay",
		"data_replay",
		"aif_workflow",
	}

	for _, entityType := range entityTypes {
		t.Run(entityType, func(t *testing.T) {
			err := updateStatus(db.ChangeFlagAck{EntityType: entityType})
			if !errors.Is(err, errSuppressUnsupportedEntityType) {
				t.Fatalf("expected suppression error, got %v", err)
			}
		})
	}
}

func TestPrepareMapOfEntityIdToRequestIdToAck(t *testing.T) {
	acks := []db.ChangeFlagAck{
		{EntityId: "entity-a", RequestId: "req-1", Id: "ack-1"},
		{EntityId: "entity-a", RequestId: "req-1", Id: "ack-2"},
		{EntityId: "entity-a", RequestId: "req-2", Id: "ack-3"},
		{EntityId: "entity-b", RequestId: "req-3", Id: "ack-4"},
	}

	result := prepareMapOfEntityIdToRequestIdToAck(acks)
	if len(result) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(result))
	}
	if len(result["entity-a"]["req-1"]) != 2 {
		t.Fatalf("expected 2 acks for req-1")
	}
	if len(result["entity-b"]["req-3"]) != 1 {
		t.Fatalf("expected 1 ack for req-3")
	}
}

func TestGetRelevantAcknowledgement(t *testing.T) {
	t.Run("prefers failure", func(t *testing.T) {
		acks := []db.ChangeFlagAck{
			{Id: "1", Status: ackPkg.StatusSuccess, Timestamp: "3"},
			{Id: "2", Status: ackPkg.StatusFailure, Timestamp: "1"},
		}
		got := getRelevantAcknowledgement(acks)
		if got.Id != "2" {
			t.Fatalf("expected failure ack, got %s", got.Id)
		}
	})

	t.Run("latest success when no failure", func(t *testing.T) {
		acks := []db.ChangeFlagAck{
			{Id: "1", Status: ackPkg.StatusSuccess, Timestamp: "1"},
			{Id: "2", Status: ackPkg.StatusSuccess, Timestamp: "3"},
		}
		got := getRelevantAcknowledgement(acks)
		if got.Id != "2" {
			t.Fatalf("expected latest success ack, got %s", got.Id)
		}
	})

	t.Run("empty returns zero value", func(t *testing.T) {
		got := getRelevantAcknowledgement(nil)
		if got.Id != "" {
			t.Fatalf("expected empty ack, got %+v", got)
		}
	})
}

func TestGetLatestEntityToRequestId(t *testing.T) {
	entityIdToChangeFlags := map[string][]db.ChangeFlagRequest{
		"entity-a": {
			{RequestId: "req-old", Timestamp: "1"},
			{RequestId: "req-new", Timestamp: "3"},
		},
		"entity-b": {
			{RequestId: "req-only", Timestamp: "2"},
		},
	}

	latest, suppressed := getLatestEntityToRequestId(entityIdToChangeFlags)
	if latest["entity-a"] != "req-new" {
		t.Fatalf("expected req-new, got %s", latest["entity-a"])
	}
	if latest["entity-b"] != "req-only" {
		t.Fatalf("expected req-only, got %s", latest["entity-b"])
	}
	if _, ok := suppressed["req-old"]; !ok {
		t.Fatalf("expected req-old to be suppressed")
	}
	if _, ok := suppressed["req-new"]; ok {
		t.Fatalf("did not expect req-new to be suppressed")
	}
}

func TestGetChangeFlagsForEntitiesEmpty(t *testing.T) {
	result, err := getChangeFlagsForEntities(map[string]map[string][]db.ChangeFlagAck{}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty map, got %v", result)
	}
}

func TestStartProcessingUsesCollector(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "log_source" SET "status"=$1 WHERE id IN ($2) AND status NOT IN ($3,$4)`)).
		WithArgs("ACTIVE", "entity-1", "ACTIVE", "DELETED").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ackMap := map[string]map[string][]db.ChangeFlagAck{
		"entity-1": {
			"req-1": {{
				Id:         "ack-1",
				EntityId:   "entity-1",
				RequestId:  "req-1",
				EntityType: utilConst.EntitySource,
				Status:     ackPkg.StatusSuccess,
			}},
		},
	}
	latest := map[string]string{"entity-1": "req-1"}

	successful, failed, suppressed := startProcessing(ackMap, latest, 100)
	if len(failed) != 0 || len(suppressed) != 0 {
		t.Fatalf("unexpected failed=%v suppressed=%v", failed, suppressed)
	}
	if _, ok := successful["req-1"]; !ok {
		t.Fatalf("expected req-1 to succeed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartProcessingMarksUnknownEntityTypeFailed(t *testing.T) {
	ackMap := map[string]map[string][]db.ChangeFlagAck{
		"entity-1": {
			"req-1": {{
				EntityId:   "entity-1",
				RequestId:  "req-1",
				EntityType: "unknown-type",
			}},
		},
	}
	latest := map[string]string{"entity-1": "req-1"}

	_, failed, suppressed := startProcessing(ackMap, latest, 100)
	if len(suppressed) != 0 {
		t.Fatalf("expected no suppressed, got %v", suppressed)
	}
	if _, ok := failed["req-1"]; !ok {
		t.Fatalf("expected req-1 to fail")
	}
}

func TestStartProcessingSkipsEntityWithoutLatestRequest(t *testing.T) {
	ackMap := map[string]map[string][]db.ChangeFlagAck{
		"entity-1": {
			"req-1": {{EntityId: "entity-1", RequestId: "req-1", EntityType: utilConst.EntitySource}},
		},
	}

	successful, failed, suppressed := startProcessing(ackMap, map[string]string{}, 100)
	if len(successful) != 0 || len(failed) != 0 || len(suppressed) != 0 {
		t.Fatalf("expected no results, got successful=%v failed=%v suppressed=%v", successful, failed, suppressed)
	}
}

func TestProcessAckPageEmptyAcks(t *testing.T) {
	if err := processAckPage(nil, 50, 100); err != nil {
		t.Fatalf("expected no error for empty page, got %v", err)
	}
}

func TestStartProcessingSuppressesUnsupportedEntity(t *testing.T) {
	ackMap := map[string]map[string][]db.ChangeFlagAck{
		"entity-1": {
			"req-1": {{
				EntityId:   "entity-1",
				RequestId:  "req-1",
				EntityType: utilConst.EntityPipeline,
			}},
		},
	}
	latest := map[string]string{"entity-1": "req-1"}

	_, _, suppressed := startProcessing(ackMap, latest, 100)
	if _, ok := suppressed["req-1"]; !ok {
		t.Fatalf("expected req-1 to be suppressed")
	}
}

func TestMarkAllAcksGroupsByRequestStatus(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "change_flag_acks" SET "process_status"=$1 WHERE id IN ($2)`)).
		WithArgs("PROCESSED", "ack-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "change_flag_acks" SET "process_status"=$1 WHERE id IN ($2)`)).
		WithArgs("ERRORED", "ack-3").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "change_flag_acks" SET "process_status"=$1 WHERE id IN ($2)`)).
		WithArgs("SUPPRESSED", "ack-2").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ackMap := map[string]map[string][]db.ChangeFlagAck{
		"entity-1": {
			"req-success": {{Id: "ack-1", RequestId: "req-success"}},
			"req-suppressed": {{Id: "ack-2", RequestId: "req-suppressed"}},
			"req-failed": {{Id: "ack-3", RequestId: "req-failed"}},
		},
	}

	markAllAcks(ackMap,
		map[string]struct{}{"req-success": {}},
		map[string]struct{}{"req-failed": {}},
		map[string]struct{}{"req-suppressed": {}},
		50,
	)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
