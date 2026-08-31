package apply

import (
	"context"
	"errors"
	"testing"

	athenastore "github.com/databahn-ai/databahn-jobs/internal/store/athena"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/google/uuid"
)

const testRegion = "us-east-1"

func testIDs() (destID, sourceID, tenantID uuid.UUID) {
	return uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		uuid.MustParse("33333333-3333-3333-3333-333333333333")
}

func mockOps(
	describeCols map[string]struct{},
	describeErr error,
	ddlErr error,
) SchemaOps {
	return SchemaOps{
		Describe: func(ctx context.Context) (map[string]struct{}, string, error) {
			if describeErr != nil {
				return nil, "", describeErr
			}
			return describeCols, "exec-1", nil
		},
		RunDDL: func(ctx context.Context, query string) error {
			return ddlErr
		},
	}
}

func testPreflightParams(destID, sourceID, tenantID uuid.UUID, fields []model.Field) PreflightParams {
	return PreflightParams{
		DestID:        destID,
		SourceID:      sourceID,
		TenantID:      tenantID,
		DispenserType: model.DispenserDatabahnStorage,
		Valid:         fields,
		Database:      "db",
		TableName:     "tbl",
		Region:        testRegion,
	}
}

func TestWithPreflight_DescribeTableNotFound(t *testing.T) {
	destID, sourceID, tenantID := testIDs()
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "col", FieldType: "string"}}),
		mockOps(nil, &athenastore.TableNotFoundError{Msg: "TABLE_NOT_FOUND"}, nil),
	)
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}

func TestWithPreflight_DescribeError(t *testing.T) {
	destID, sourceID, tenantID := testIDs()
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "col", FieldType: "string"}}),
		mockOps(nil, errors.New("connection refused"), nil),
	)
	if err == nil || model.IsTableNotFound(err) {
		t.Fatalf("expected describe error, got %v", err)
	}
}

func TestWithPreflight_AllAlreadyPresent(t *testing.T) {
	withMarkStub(t, func(ctx context.Context, ids []int64, phase string) error {
		if phase != "already_present" || len(ids) != 1 || ids[0] != 1 {
			t.Fatalf("unexpected mark call: phase=%s ids=%v", phase, ids)
		}
		return nil
	})
	destID, sourceID, tenantID := testIDs()
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "existing_col", FieldType: "string"}}),
		mockOps(map[string]struct{}{"existing_col": {}}, nil, nil),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWithPreflight_AddsMissingColumns(t *testing.T) {
	phases := []string{}
	withMarkStub(t, func(ctx context.Context, ids []int64, phase string) error {
		phases = append(phases, phase)
		return nil
	})
	destID, sourceID, tenantID := testIDs()
	var ddlRan bool
	ops := SchemaOps{
		Describe: func(ctx context.Context) (map[string]struct{}, string, error) {
			return map[string]struct{}{"old_col": {}}, "exec-2", nil
		},
		RunDDL: func(ctx context.Context, query string) error {
			ddlRan = true
			return nil
		},
	}
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "new_col", FieldType: "long"}}),
		ops,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ddlRan {
		t.Fatal("expected DDL to run")
	}
	if len(phases) != 1 || phases[0] != "after_ddl" {
		t.Fatalf("expected after_ddl mark, got %v", phases)
	}
}

func TestWithPreflight_DDLFailure(t *testing.T) {
	destID, sourceID, tenantID := testIDs()
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "new_col", FieldType: "string"}}),
		mockOps(map[string]struct{}{}, nil, errors.New("ddl failed")),
	)
	if err == nil {
		t.Fatal("expected ddl error")
	}
}

func TestWithPreflight_EmptyDescribeColumns(t *testing.T) {
	withMarkStub(t, func(ctx context.Context, ids []int64, phase string) error { return nil })
	destID, sourceID, tenantID := testIDs()
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "col", FieldType: "string"}}),
		mockOps(map[string]struct{}{}, nil, nil),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWithPreflight_MarkAppliedFailure(t *testing.T) {
	withMarkStub(t, func(ctx context.Context, ids []int64, phase string) error {
		return errors.New("mark failed")
	})
	destID, sourceID, tenantID := testIDs()
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "col", FieldType: "string"}}),
		mockOps(map[string]struct{}{"col": {}}, nil, nil),
	)
	if err == nil {
		t.Fatal("expected mark applied error")
	}
}

func TestPlatformOps_ReturnsHandlers(t *testing.T) {
	ops := PlatformOps(testRegion, "db", "tbl", "s3://bucket/out/")
	if ops.Describe == nil || ops.RunDDL == nil {
		t.Fatal("expected non-nil handlers")
	}
}

func TestWithPreflight_MarkAfterDDLFails(t *testing.T) {
	calls := 0
	withMarkStub(t, func(ctx context.Context, ids []int64, phase string) error {
		calls++
		if phase == "after_ddl" {
			return errors.New("mark failed")
		}
		return nil
	})
	destID, sourceID, tenantID := testIDs()
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "new_col", FieldType: "string"}}),
		mockOps(map[string]struct{}{}, nil, nil),
	)
	if err == nil {
		t.Fatal("expected mark error after ddl")
	}
	if calls != 1 {
		t.Fatalf("expected one mark call before failure, got %d", calls)
	}
}

func TestWithPreflight_InvalidColumnDDL(t *testing.T) {
	destID, sourceID, tenantID := testIDs()
	err := WithPreflight(
		context.Background(),
		testPreflightParams(destID, sourceID, tenantID, []model.Field{{ID: 1, Name: "bad-name", FieldType: "string"}}),
		mockOps(map[string]struct{}{}, nil, nil),
	)
	if err == nil {
		t.Fatal("expected ddl build error")
	}
}

func TestCustomerOps_ReturnsHandlers(t *testing.T) {
	ops := CustomerOps(nil, "db", "tbl", "s3://bucket/out/")
	if ops.Describe == nil || ops.RunDDL == nil {
		t.Fatal("expected non-nil handlers")
	}
}
