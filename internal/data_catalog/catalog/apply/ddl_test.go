package apply

import (
	"strings"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
)

func TestBuildAddColumnsDDL_SingleColumn(t *testing.T) {
	q, err := BuildAddColumnsDDL("databahn_tenant_abc", "events", []model.Field{
		{ID: 1, Name: "src_ip", FieldType: "string"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "ALTER TABLE `databahn_tenant_abc`.`events` ADD COLUMNS (`src_ip` string)"
	if q != want {
		t.Fatalf("got %q, want %q", q, want)
	}
}

func TestBuildAddColumnsDDL_MultipleColumns(t *testing.T) {
	q, err := BuildAddColumnsDDL("db", "tbl", []model.Field{
		{ID: 1, Name: "a", FieldType: "long"},
		{ID: 2, Name: "b", FieldType: "double"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "`a` bigint") || !strings.Contains(q, "`b` double") {
		t.Fatalf("unexpected query: %s", q)
	}
}

func TestBuildAddColumnsDDL_InvalidColumnName(t *testing.T) {
	_, err := BuildAddColumnsDDL("db", "tbl", []model.Field{{Name: "bad-name"}})
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestBuildAddColumnsDDL_InvalidDatabaseName(t *testing.T) {
	_, err := BuildAddColumnsDDL("bad-db", "tbl", []model.Field{{Name: "col"}})
	if err == nil {
		t.Fatal("expected error for invalid database name")
	}
}

func TestBuildAddColumnsDDL_InvalidTableName(t *testing.T) {
	_, err := BuildAddColumnsDDL("db", "bad-table", []model.Field{{Name: "col"}})
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestBuildAddColumnsDDL_TypeMapping(t *testing.T) {
	cases := []struct {
		fieldType string
		want      string
	}{
		{"long", "bigint"},
		{"string", "string"},
		{"double", "double"},
		{"boolean", "boolean"},
		{"unknown", "string"},
	}
	for _, tc := range cases {
		q, err := BuildAddColumnsDDL("db", "tbl", []model.Field{{Name: "c", FieldType: tc.fieldType}})
		if err != nil {
			t.Fatalf("fieldType %s: %v", tc.fieldType, err)
		}
		if !strings.Contains(q, "`c` "+tc.want) {
			t.Fatalf("fieldType %s: got %q", tc.fieldType, q)
		}
	}
}
