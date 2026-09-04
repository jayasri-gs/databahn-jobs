package acknowledgement

import (
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	ackPkg "github.com/databahn-ai/common-utils/ack"
	utilConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/databahn-jobs/internal/acknowledgement/db"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/dbtest"
)

func TestEntityUpdateSpecForAck(t *testing.T) {
	tests := []struct {
		name      string
		ack       db.ChangeFlagAck
		wantTable string
		wantGuard entityUpdateGuard
		wantErr   error
	}{
		{
			name:      "destination prefix",
			ack:       db.ChangeFlagAck{EntityType: "destination_s3", Status: ackPkg.StatusSuccess},
			wantTable: "destination",
			wantGuard: guardExcludeDeleted,
		},
		{
			name:      "source",
			ack:       db.ChangeFlagAck{EntityType: utilConst.EntitySource, Status: ackPkg.StatusSuccess},
			wantTable: "log_source",
			wantGuard: guardExcludeDeleted,
		},
		{
			name:      "lookup",
			ack:       db.ChangeFlagAck{EntityType: utilConst.EntityLookup, Status: ackPkg.StatusSuccess},
			wantTable: "lookup",
			wantGuard: guardExcludeStatusOnly,
		},
		{
			name:    "unsupported pipeline",
			ack:     db.ChangeFlagAck{EntityType: utilConst.EntityPipeline},
			wantErr: errSuppressUnsupportedEntityType,
		},
		{
			name:    "unknown type",
			ack:     db.ChangeFlagAck{EntityType: "unknown-type"},
			wantErr: errors.New("Ack does not support entity type:unknown-type"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := entityUpdateSpecForAck(tt.ack)
			if tt.wantErr != nil {
				if err == nil || err.Error() != tt.wantErr.Error() {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if spec.table != tt.wantTable || spec.guardKind != tt.wantGuard {
				t.Fatalf("unexpected spec: %+v", spec)
			}
		})
	}
}

func TestEntityUpdateCollectorFlushSuccess(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "log_source" SET "status"=$1 WHERE id IN ($2,$3) AND status NOT IN ($4,$5)`)).
		WithArgs("ACTIVE", "entity-1", "entity-2", "ACTIVE", "DELETED").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	collector := newEntityUpdateCollector()
	spec := entityUpdateSpec{table: "log_source", status: "ACTIVE", guardKind: guardExcludeDeleted}
	collector.add(spec, "entity-1", "req-1")
	collector.add(spec, "entity-2", "req-2")

	successful, failed := collector.flush(100)
	if len(failed) != 0 {
		t.Fatalf("expected no failures, got %v", failed)
	}
	if len(successful) != 2 {
		t.Fatalf("expected 2 successes, got %d", len(successful))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEntityUpdateCollectorFlushBatchFallback(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "log_source" SET "status"=$1 WHERE id IN ($2,$3) AND status NOT IN ($4,$5)`)).
		WithArgs("ACTIVE", "entity-1", "entity-2", "ACTIVE", "DELETED").
		WillReturnError(errors.New("batch failed"))
	mock.ExpectRollback()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "log_source" SET "status"=$1 WHERE id = $2 AND status NOT IN ($3,$4)`)).
		WithArgs("ACTIVE", "entity-1", "ACTIVE", "DELETED").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "log_source" SET "status"=$1 WHERE id = $2 AND status NOT IN ($3,$4)`)).
		WithArgs("ACTIVE", "entity-2", "ACTIVE", "DELETED").
		WillReturnError(errors.New("single failed"))
	mock.ExpectRollback()

	collector := newEntityUpdateCollector()
	spec := entityUpdateSpec{table: "log_source", status: "ACTIVE", guardKind: guardExcludeDeleted}
	collector.add(spec, "entity-1", "req-1")
	collector.add(spec, "entity-2", "req-2")

	successful, failed := collector.flush(100)
	if len(successful) != 1 {
		t.Fatalf("expected 1 success, got %d", len(successful))
	}
	if _, ok := successful["req-1"]; !ok {
		t.Fatalf("expected req-1 to succeed")
	}
	if _, ok := failed["req-2"]; !ok {
		t.Fatalf("expected req-2 to fail")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEntityUpdateCollectorFlushBatchesBySize(t *testing.T) {
	_, mock := dbtest.MockPostgres(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "lookup" SET "status"=$1 WHERE id IN ($2) AND status != $3`)).
		WithArgs("ACTIVE", "entity-1", "ACTIVE").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "lookup" SET "status"=$1 WHERE id IN ($2) AND status != $3`)).
		WithArgs("ACTIVE", "entity-2", "ACTIVE").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	collector := newEntityUpdateCollector()
	spec := entityUpdateSpec{table: "lookup", status: "ACTIVE", guardKind: guardExcludeStatusOnly}
	collector.add(spec, "entity-1", "req-1")
	collector.add(spec, "entity-2", "req-2")

	successful, failed := collector.flush(1)
	if len(failed) != 0 || len(successful) != 2 {
		t.Fatalf("expected 2 successes, got successful=%d failed=%d", len(successful), len(failed))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
