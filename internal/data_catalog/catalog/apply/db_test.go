package apply

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/dbtest"
)

func useDefaultMarkApplied(t *testing.T) {
	t.Helper()
	oldMark := markCatalogApplied
	oldUpdate := updateCatalogAppliedRows
	markCatalogApplied = defaultMarkCatalogApplied
	updateCatalogAppliedRows = defaultUpdateCatalogAppliedRows
	t.Cleanup(func() {
		markCatalogApplied = oldMark
		updateCatalogAppliedRows = oldUpdate
	})
}

func TestDefaultUpdateCatalogAppliedRows_Success(t *testing.T) {
	useDefaultMarkApplied(t)
	_, mock := dbtest.MockPostgres(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "data_catalog"`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := MarkApplied(context.Background(), []int64{1}, "after_ddl"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultUpdateCatalogAppliedRows_Error(t *testing.T) {
	useDefaultMarkApplied(t)
	_, mock := dbtest.MockPostgres(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "data_catalog"`)).
		WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectRollback()

	err := MarkApplied(context.Background(), []int64{1}, "after_ddl")
	if err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
