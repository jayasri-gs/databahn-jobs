package apply

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
)

func withMarkStub(t *testing.T, fn func(ctx context.Context, ids []int64, phase string) error) {
	t.Helper()
	old := markCatalogApplied
	markCatalogApplied = fn
	t.Cleanup(func() { markCatalogApplied = old })
}

func TestPartitionByPresence_AllMissing(t *testing.T) {
	fields := []model.Field{
		{ID: 1, Name: "a"},
		{ID: 2, Name: "b"},
		{ID: 3, Name: "c"},
	}
	present, missing := PartitionByPresence(fields, map[string]struct{}{})
	if len(present) != 0 || len(missing) != 3 {
		t.Fatalf("expected 0 present / 3 missing, got %d / %d", len(present), len(missing))
	}
}

func TestPartitionByPresence_AllPresent(t *testing.T) {
	fields := []model.Field{
		{ID: 1, Name: "Foo"},
		{ID: 2, Name: "BAR"},
		{ID: 3, Name: "baz"},
	}
	existing := map[string]struct{}{
		"foo": {},
		"bar": {},
		"baz": {},
	}
	present, missing := PartitionByPresence(fields, existing)
	if len(present) != 3 || len(missing) != 0 {
		t.Fatalf("expected 3 present / 0 missing, got %d / %d", len(present), len(missing))
	}
}

func TestPartitionByPresence_PartialOverlap(t *testing.T) {
	fields := make([]model.Field, 10)
	for i := range fields {
		fields[i] = model.Field{ID: int64(i + 1), Name: fmt.Sprintf("field_%d", i)}
	}
	existing := map[string]struct{}{
		"field_0": {},
		"field_1": {},
		"field_2": {},
		"field_3": {},
	}
	present, missing := PartitionByPresence(fields, existing)
	if len(present) != 4 || len(missing) != 6 {
		t.Fatalf("expected 4 present / 6 missing, got %d / %d", len(present), len(missing))
	}
}

func TestPartitionByPresence_CaseInsensitive(t *testing.T) {
	fields := []model.Field{{ID: 1, Name: "SrcIp"}}
	existing := map[string]struct{}{"srcip": {}}
	present, missing := PartitionByPresence(fields, existing)
	if len(present) != 1 || len(missing) != 0 {
		t.Fatalf("expected SrcIp in present, got present=%v missing=%v", model.FieldNames(present), model.FieldNames(missing))
	}
}

func TestPartitionByPresence_EmptyInput(t *testing.T) {
	present, missing := PartitionByPresence(nil, map[string]struct{}{"a": {}})
	if len(present) != 0 || len(missing) != 0 {
		t.Fatalf("expected empty slices, got %d / %d", len(present), len(missing))
	}
}

func TestMarkApplied_EmptyIDs(t *testing.T) {
	if err := MarkApplied(context.Background(), nil, "test"); err != nil {
		t.Fatal(err)
	}
}

func TestMarkApplied_Success(t *testing.T) {
	withMarkStub(t, func(ctx context.Context, ids []int64, phase string) error {
		if phase != "test" || len(ids) != 1 || ids[0] != 1 {
			t.Fatalf("unexpected args: phase=%s ids=%v", phase, ids)
		}
		return nil
	})
	if err := MarkApplied(context.Background(), []int64{1}, "test"); err != nil {
		t.Fatal(err)
	}
}

func TestMarkApplied_DBError(t *testing.T) {
	withMarkStub(t, func(ctx context.Context, ids []int64, phase string) error {
		return errors.New("db down")
	})
	err := MarkApplied(context.Background(), []int64{1}, "test")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDefaultMarkCatalogApplied_ErrorFormatting(t *testing.T) {
	oldUpdate := updateCatalogAppliedRows
	oldMark := markCatalogApplied
	updateCatalogAppliedRows = func(ctx context.Context, ids []int64) error {
		return errors.New("db down")
	}
	markCatalogApplied = defaultMarkCatalogApplied
	t.Cleanup(func() {
		updateCatalogAppliedRows = oldUpdate
		markCatalogApplied = oldMark
	})

	err := MarkApplied(context.Background(), []int64{1, 2}, "after_ddl")
	if err == nil || !strings.Contains(err.Error(), "after_ddl") {
		t.Fatalf("expected formatted error, got %v", err)
	}
}