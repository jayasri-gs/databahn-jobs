package model

import (
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func TestTableNotFoundError_Error(t *testing.T) {
	err := NewTableNotFoundError("missing %s", "table")
	if err.Error() != "missing table" {
		t.Fatalf("got %q", err.Error())
	}
}

func TestGroupKey(t *testing.T) {
	dest := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	source := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	tenant := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	got := GroupKey(dest, source, tenant)
	want := dest.String() + "|" + source.String() + "|" + tenant.String()
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestGroupFields(t *testing.T) {
	dest := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	source := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	tenant := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	fields := GroupFields(dest, source, tenant, DispenserDatabahnStorage)
	if len(fields) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(fields))
	}
	if fields[3].Key != "dispenser_type" || fields[3].String != DispenserDatabahnStorage {
		t.Fatalf("unexpected dispenser field: %v", fields[3])
	}
	zap.NewNop().Info("ok", fields...)
}

func TestAthenaType(t *testing.T) {
	cases := map[string]string{
		"long":    "bigint",
		"string":  "string",
		"double":  "double",
		"boolean": "boolean",
		"other":   "string",
	}
	for in, want := range cases {
		if got := AthenaType(in); got != want {
			t.Fatalf("AthenaType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestField_TableName(t *testing.T) {
	if (Field{}).TableName() != "data_catalog" {
		t.Fatal("unexpected table name")
	}
}

func TestIsTableNotFound_Nil(t *testing.T) {
	if IsTableNotFound(nil) {
		t.Fatal("nil should not match")
	}
}
