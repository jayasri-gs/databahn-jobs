package validate

import (
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
)

func fieldNames(fs []model.Field) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Name
	}
	return out
}

func reasonByName(inv []InvalidField) map[string]string {
	m := map[string]string{}
	for _, i := range inv {
		m[i.Field.Name] = i.Reason
	}
	return m
}

func TestPartitionCatalogFields_CleanPassthrough(t *testing.T) {
	in := []model.Field{
		{ID: 1, Name: "src_ip"},
		{ID: 2, Name: "dst_port"},
	}
	valid, invalid := PartitionCatalogFields(in, nil)
	if len(valid) != 2 || len(invalid) != 0 {
		t.Fatalf("expected 2 valid / 0 invalid, got %d / %d", len(valid), len(invalid))
	}
}

func TestPartitionCatalogFields_LeadingUnderscore(t *testing.T) {
	in := []model.Field{
		{ID: 1, Name: "_hidden"},
		{ID: 2, Name: "good"},
	}
	valid, invalid := PartitionCatalogFields(in, nil)
	if len(valid) != 1 || valid[0].Name != "good" {
		t.Fatalf("expected only 'good' valid, got %v", fieldNames(valid))
	}
	if reasonByName(invalid)["_hidden"] != "leading_underscore" {
		t.Fatalf("expected leading_underscore reason, got %v", reasonByName(invalid))
	}
}

func TestPartitionCatalogFields_ReservedKeywordMixedCase(t *testing.T) {
	in := []model.Field{
		{ID: 1, Name: "Order"},
		{ID: 2, Name: "amount"},
	}
	valid, invalid := PartitionCatalogFields(in, nil)
	if len(valid) != 1 || valid[0].Name != "amount" {
		t.Fatalf("expected only 'amount' valid, got %v", fieldNames(valid))
	}
	if reasonByName(invalid)["Order"] != "reserved_keyword" {
		t.Fatalf("expected reserved_keyword reason, got %v", reasonByName(invalid))
	}
}

func TestPartitionCatalogFields_CacheKeyword(t *testing.T) {
	in := []model.Field{{ID: 1, Name: "cache"}}
	valid, invalid := PartitionCatalogFields(in, nil)
	if len(valid) != 0 || reasonByName(invalid)["cache"] != "reserved_keyword" {
		t.Fatalf("expected 'cache' rejected as reserved_keyword, got valid=%v invalid=%v", fieldNames(valid), reasonByName(invalid))
	}
}

func TestPartitionCatalogFields_InBatchDuplicateOldestWins(t *testing.T) {
	in := []model.Field{
		{ID: 9, Name: "FOO"},
		{ID: 3, Name: "foo"},
	}
	valid, invalid := PartitionCatalogFields(in, nil)
	if len(valid) != 1 || valid[0].ID != 3 {
		t.Fatalf("expected oldest id=3 to survive, got %v", valid)
	}
	if len(invalid) != 1 || invalid[0].Field.ID != 9 || invalid[0].Reason != "duplicate" {
		t.Fatalf("expected id=9 invalid duplicate, got %v", invalid)
	}
}

func TestPartitionCatalogFields_DuplicateAgainstApplied(t *testing.T) {
	in := []model.Field{
		{ID: 5, Name: "SrcIp"},
		{ID: 6, Name: "new_col"},
	}
	valid, invalid := PartitionCatalogFields(in, []string{"srcip"})
	if len(valid) != 1 || valid[0].Name != "new_col" {
		t.Fatalf("expected only 'new_col' valid, got %v", fieldNames(valid))
	}
	if reasonByName(invalid)["SrcIp"] != "duplicate" {
		t.Fatalf("expected duplicate reason for SrcIp, got %v", reasonByName(invalid))
	}
}

func TestPartitionCatalogFields_AllInvalid(t *testing.T) {
	in := []model.Field{
		{ID: 1, Name: "_x"},
		{ID: 2, Name: "select"},
	}
	valid, invalid := PartitionCatalogFields(in, nil)
	if len(valid) != 0 || len(invalid) != 2 {
		t.Fatalf("expected 0 valid / 2 invalid, got %d / %d", len(valid), len(invalid))
	}
}
