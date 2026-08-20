package validate

import (
	"context"
	"errors"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/google/uuid"
)

func testIDs() (destID, sourceID, tenantID uuid.UUID) {
	return uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		uuid.MustParse("33333333-3333-3333-3333-333333333333")
}

func withCleanDeps(t *testing.T, pluckFn func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, []string) ([]string, error), deleteFn func(context.Context, []int64) error) {
	t.Helper()
	oldPluck := pluckAppliedNames
	oldDelete := deleteInvalidRows
	pluckAppliedNames = pluckFn
	deleteInvalidRows = deleteFn
	t.Cleanup(func() {
		pluckAppliedNames = oldPluck
		deleteInvalidRows = oldDelete
	})
}

func TestCleanFields_ValidOnly(t *testing.T) {
	withCleanDeps(t,
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, incomingLower []string) ([]string, error) {
			return nil, nil
		},
		func(ctx context.Context, invalidIDs []int64) error { return nil },
	)
	destID, sourceID, tenantID := testIDs()
	valid, err := CleanFields(context.Background(), destID, sourceID, tenantID, model.DispenserDatabahnStorage,
		[]model.Field{{ID: 1, Name: "src_ip", FieldType: "string"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(valid) != 1 || valid[0].Name != "src_ip" {
		t.Fatalf("unexpected valid: %v", valid)
	}
}

func TestCleanFields_DeletesInvalid(t *testing.T) {
	var deleted []int64
	withCleanDeps(t,
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, incomingLower []string) ([]string, error) {
			return nil, nil
		},
		func(ctx context.Context, invalidIDs []int64) error {
			deleted = append(deleted, invalidIDs...)
			return nil
		},
	)
	destID, sourceID, tenantID := testIDs()
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
	if len(deleted) != 1 || deleted[0] != 1 {
		t.Fatalf("expected invalid id 1 deleted, got %v", deleted)
	}
}

func TestCleanFields_DuplicateAgainstApplied(t *testing.T) {
	withCleanDeps(t,
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, incomingLower []string) ([]string, error) {
			return []string{"srcip"}, nil
		},
		func(ctx context.Context, invalidIDs []int64) error { return nil },
	)
	destID, sourceID, tenantID := testIDs()
	valid, err := CleanFields(context.Background(), destID, sourceID, tenantID, model.DispenserDatabahnStorage,
		[]model.Field{
			{ID: 11, Name: "SrcIp", FieldType: "string"},
			{ID: 12, Name: "new_col", FieldType: "string"},
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(valid) != 1 || valid[0].Name != "new_col" {
		t.Fatalf("unexpected valid: %v", valid)
	}
}

func TestCleanFields_AllInvalid(t *testing.T) {
	withCleanDeps(t,
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, incomingLower []string) ([]string, error) {
			return nil, nil
		},
		func(ctx context.Context, invalidIDs []int64) error { return nil },
	)
	destID, sourceID, tenantID := testIDs()
	valid, err := CleanFields(context.Background(), destID, sourceID, tenantID, model.DispenserDatabahnStorage,
		[]model.Field{{ID: 1, Name: "_bad", FieldType: "string"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(valid) != 0 {
		t.Fatalf("expected no valid fields, got %v", valid)
	}
}

func TestCleanFields_PluckError(t *testing.T) {
	withCleanDeps(t,
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, incomingLower []string) ([]string, error) {
			return nil, errors.New("pluck failed")
		},
		func(ctx context.Context, invalidIDs []int64) error { return nil },
	)
	destID, sourceID, tenantID := testIDs()
	_, err := CleanFields(context.Background(), destID, sourceID, tenantID, model.DispenserDatabahnStorage,
		[]model.Field{{ID: 1, Name: "col"}})
	if err == nil {
		t.Fatal("expected pluck error")
	}
}

func TestCleanFields_DeleteError(t *testing.T) {
	withCleanDeps(t,
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, incomingLower []string) ([]string, error) {
			return nil, nil
		},
		func(ctx context.Context, invalidIDs []int64) error { return errors.New("delete failed") },
	)
	destID, sourceID, tenantID := testIDs()
	_, err := CleanFields(context.Background(), destID, sourceID, tenantID, model.DispenserDatabahnStorage,
		[]model.Field{{ID: 1, Name: "select", FieldType: "string"}})
	if err == nil {
		t.Fatal("expected delete error")
	}
}
