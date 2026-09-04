package db

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/constants"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/dbtest"
)

func TestGetDistinctEntityIdsToProcessWithCursor(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	olderThan := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	lookback := time.Date(2026, 9, 1, 11, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT DISTINCT entity_id FROM "change_flag_acks"`).
		WithArgs(constants.StatusPending, constants.StatusErrored, olderThan, lookback, "entity-a", 2).
		WillReturnRows(sqlmock.NewRows([]string{"entity_id"}).AddRow("entity-b").AddRow("entity-c"))

	ids, err := GetDistinctEntityIdsToProcess(olderThan, &lookback, "entity-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "entity-b" || ids[1] != "entity-c" {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetDistinctEntityIdsToProcessFirstPage(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	olderThan := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT DISTINCT entity_id FROM "change_flag_acks"`).
		WillReturnRows(sqlmock.NewRows([]string{"entity_id"}).AddRow("entity-a"))

	ids, err := GetDistinctEntityIdsToProcess(olderThan, nil, "", 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "entity-a" {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetChangeFlagsByEntityIdsEmpty(t *testing.T) {
	acks, err := GetChangeFlagsByEntityIds(nil, time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if acks != nil {
		t.Fatalf("expected nil acks, got %v", acks)
	}
}

func TestGetChangeFlagsByEntityIds(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	olderThan := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	entityIds := []string{"entity-a", "entity-b"}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "change_flag_acks" WHERE (process_status IN ($1,$2) AND timestamp < $3) AND entity_id IN ($4,$5)`)).
		WithArgs(constants.StatusPending, constants.StatusErrored, olderThan, "entity-a", "entity-b").
		WillReturnRows(sqlmock.NewRows([]string{"id", "request_id", "entity_id", "status", "entity_type", "entity_version", "service_name", "tenant_id", "action", "timestamp", "error", "process_status"}).
			AddRow("ack-1", "req-1", "entity-a", "SUCCESS", "source", "", "", "", "", "", "", "PENDING"))

	acks, err := GetChangeFlagsByEntityIds(entityIds, olderThan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 1 || acks[0].Id != "ack-1" {
		t.Fatalf("unexpected acks: %+v", acks)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMarkAcksProcessedSingleUpdate(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "change_flag_acks" SET "process_status"=$1 WHERE id IN ($2)`)).
		WithArgs(constants.StatusProcessed, "ack-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := MarkAcksProcessed([]string{"ack-1"}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
