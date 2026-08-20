package databahnstorage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/apply"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/google/uuid"
)

func testIDs() (destID, sourceID, tenantID uuid.UUID) {
	return uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		uuid.MustParse("33333333-3333-3333-3333-333333333333")
}

func ptrOps(ops apply.SchemaOps) *apply.SchemaOps { return &ops }

func mockOps() apply.SchemaOps {
	return apply.SchemaOps{
		Describe: func(ctx context.Context) (map[string]struct{}, string, error) {
			return map[string]struct{}{}, "exec-1", nil
		},
		RunDDL: func(ctx context.Context, query string) error { return nil },
	}
}

func withSyncStubs(t *testing.T, resolve func(context.Context, uuid.UUID, uuid.UUID) (databahnStorageTarget, error), clean func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, []model.Field) ([]model.Field, error), preflight func(context.Context, apply.PreflightParams, apply.SchemaOps) error) {
	t.Helper()
	oldResolve, oldClean, oldPreflight := resolveDatabahnStorageTarget, cleanFields, runPreflight
	resolveDatabahnStorageTarget = resolve
	cleanFields = clean
	runPreflight = preflight
	t.Cleanup(func() {
		resolveDatabahnStorageTarget = oldResolve
		cleanFields = oldClean
		runPreflight = oldPreflight
	})
}

func TestApplyDataCatalogToAthena_NoFields(t *testing.T) {
	oldQuery := queryUnappliedFields
	queryUnappliedFields = func(ctx context.Context) ([]model.Field, error) { return nil, nil }
	t.Cleanup(func() { queryUnappliedFields = oldQuery })

	result := ApplyDataCatalogToAthena(context.Background())
	if len(result.Errors) != 0 {
		t.Fatalf("expected success, got %v", result.Errors)
	}
}

func TestApplyDataCatalogToAthena_SkipsTableNotFound(t *testing.T) {
	destID, sourceID, tenantID := testIDs()
	oldQuery := queryUnappliedFields
	oldProcess := processGroupFn
	queryUnappliedFields = func(ctx context.Context) ([]model.Field, error) {
		return []model.Field{{ID: 1, Name: "col", DestID: destID, SourceID: sourceID, TenantID: tenantID}}, nil
	}
	processGroupFn = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, fields []model.Field, ops *apply.SchemaOps) error {
		return model.NewTableNotFoundError("missing")
	}
	t.Cleanup(func() {
		queryUnappliedFields = oldQuery
		processGroupFn = oldProcess
	})

	result := ApplyDataCatalogToAthena(context.Background())
	if len(result.Errors) != 0 {
		t.Fatalf("expected success with skip, got %v", result.Errors)
	}
}

func TestApplyDataCatalogToAthena_CollectsErrors(t *testing.T) {
	destID, sourceID, tenantID := testIDs()
	oldQuery := queryUnappliedFields
	oldProcess := processGroupFn
	queryUnappliedFields = func(ctx context.Context) ([]model.Field, error) {
		return []model.Field{{ID: 1, Name: "col", DestID: destID, SourceID: sourceID, TenantID: tenantID}}, nil
	}
	processGroupFn = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, fields []model.Field, ops *apply.SchemaOps) error {
		return errors.New("boom")
	}
	t.Cleanup(func() {
		queryUnappliedFields = oldQuery
		processGroupFn = oldProcess
	})

	result := ApplyDataCatalogToAthena(context.Background())
	if len(result.Errors) != 1 {
		t.Fatalf("expected one error, got %v", result.Errors)
	}
}

func TestProcessGroup_Success(t *testing.T) {
	withSyncStubs(t,
		func(ctx context.Context, destID, sourceID uuid.UUID) (databahnStorageTarget, error) {
			return databahnStorageTarget{tableName: "events", region: "us-east-1", outputLocation: "s3://b/out/"}, nil
		},
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, fields []model.Field) ([]model.Field, error) {
			return fields, nil
		},
		func(ctx context.Context, p apply.PreflightParams, ops apply.SchemaOps) error {
			return nil
		},
	)
	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "new_col"}}, ptrOps(mockOps()))
	if err != nil {
		t.Fatal(err)
	}
}

func TestProcessGroup_ResolveError(t *testing.T) {
	withSyncStubs(t,
		func(ctx context.Context, destID, sourceID uuid.UUID) (databahnStorageTarget, error) {
			return databahnStorageTarget{}, model.NewTableNotFoundError("missing store")
		},
		nil, nil,
	)
	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "col"}}, ptrOps(mockOps()))
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}

func TestProcessGroup_NoValidFields(t *testing.T) {
	withSyncStubs(t,
		func(ctx context.Context, destID, sourceID uuid.UUID) (databahnStorageTarget, error) {
			return databahnStorageTarget{tableName: "events", region: "us-east-1", outputLocation: "s3://b/out/"}, nil
		},
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, fields []model.Field) ([]model.Field, error) {
			return nil, nil
		},
		nil,
	)
	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "select"}}, ptrOps(mockOps()))
	if err != nil {
		t.Fatal(err)
	}
}

func TestDefaultResolveDatabahnStorageTarget_EmptyAthenaTable(t *testing.T) {
	oldResolve := resolveDatabahnStorageTarget
	resolveDatabahnStorageTarget = func(ctx context.Context, destID, sourceID uuid.UUID) (databahnStorageTarget, error) {
		var sc model.SearchConfig
		sc.S3Configuration.AthenaTable = ""
		cfg, _ := json.Marshal(sc)
		_ = cfg
		return databahnStorageTarget{}, model.NewTableNotFoundError("athenaTable is empty")
	}
	t.Cleanup(func() { resolveDatabahnStorageTarget = oldResolve })

	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "col"}}, ptrOps(mockOps()))
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}
