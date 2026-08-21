package validate

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/dbtest"
	"github.com/google/uuid"
)

func useDefaultCleanDeps(t *testing.T) {
	t.Helper()
	oldPluck := pluckAppliedNames
	oldDelete := deleteInvalidRows
	oldClean := cleanCatalogFields
	pluckAppliedNames = defaultPluckAppliedNames
	deleteInvalidRows = defaultDeleteInvalidRows
	cleanCatalogFields = defaultCleanCatalogFields
	t.Cleanup(func() {
		pluckAppliedNames = oldPluck
		deleteInvalidRows = oldDelete
		cleanCatalogFields = oldClean
	})
}

func TestDefaultCleanCatalogFields_WithMockDB(t *testing.T) {
	useDefaultCleanDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	sourceID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	tenantID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	mock.ExpectQuery(`SELECT "name" FROM "data_catalog"`).
		WillReturnRows(sqlmock.NewRows([]string{"name"}))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM "data_catalog"`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	valid, err := CleanFields(context.Background(), destID, sourceID, tenantID, model.DispenserDatabahnStorage,
		[]model.Field{
			{ID: 1, Name: "select", FieldType: "string"},
			{ID: 2, Name: "good_col", FieldType: "string"},
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(valid) != 1 || valid[0].Name != "good_col" {
		t.Fatalf("unexpected valid: %v", valid)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultPluckAppliedNames_Error(t *testing.T) {
	useDefaultCleanDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	sourceID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	tenantID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	mock.ExpectQuery(`SELECT "name" FROM "data_catalog"`).
		WillReturnError(sqlmock.ErrCancelled)

	_, err := CleanFields(context.Background(), destID, sourceID, tenantID, model.DispenserDatabahnStorage,
		[]model.Field{{ID: 1, Name: "col", FieldType: "string"}})
	if err == nil {
		t.Fatal("expected pluck error")
	}
}
