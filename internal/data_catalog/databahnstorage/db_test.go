package databahnstorage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/dbtest"
)

func useDefaultDBDeps(t *testing.T) {
	t.Helper()
	oldQuery := queryUnappliedFields
	oldResolve := resolveDatabahnStorageTarget
	queryUnappliedFields = defaultQueryUnappliedFields
	resolveDatabahnStorageTarget = defaultResolveDatabahnStorageTarget
	t.Cleanup(func() {
		queryUnappliedFields = oldQuery
		resolveDatabahnStorageTarget = oldResolve
	})
}

func TestDefaultQueryUnappliedFields_WithMockDB(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, sourceID, tenantID := testIDs()

	mock.ExpectQuery(`SELECT .* FROM "data_catalog"`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "field_type", "source_id", "destination_id", "tenant_id"}).
			AddRow(1, "col", "string", sourceID, destID, tenantID))

	fields, err := queryUnappliedFields(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 || fields[0].Name != "col" {
		t.Fatalf("unexpected fields: %v", fields)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultResolveDatabahnStorageTarget_WithMockDB(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, sourceID, _ := testIDs()

	var sc model.SearchConfig
	sc.S3Configuration.AthenaTable = "events"
	sc.S3Configuration.DatabahnStorageRegion = "us-east-1"
	sc.S3Configuration.S3Location = "s3://my-bucket/data/"
	cfg, _ := json.Marshal(sc)

	mock.ExpectQuery(`SELECT id FROM search_data_store`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("store-1"))
	mock.ExpectQuery(`SELECT search_configuration FROM search_data_set`).
		WillReturnRows(sqlmock.NewRows([]string{"search_configuration"}).AddRow(string(cfg)))

	target, err := resolveDatabahnStorageTarget(context.Background(), destID, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if target.tableName != "events" || target.region != "us-east-1" {
		t.Fatalf("unexpected target: %+v", target)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultResolveDatabahnStorageTarget_StoreNotFound(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, sourceID, _ := testIDs()

	mock.ExpectQuery(`SELECT id FROM search_data_store`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	_, err := resolveDatabahnStorageTarget(context.Background(), destID, sourceID)
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}
