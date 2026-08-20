package model

import "testing"

func TestFieldIDs(t *testing.T) {
	fields := []Field{{ID: 10}, {ID: 20}}
	got := FieldIDs(fields)
	if len(got) != 2 || got[0] != 10 || got[1] != 20 {
		t.Fatalf("unexpected ids: %v", got)
	}
}

func TestFieldNames(t *testing.T) {
	fields := []Field{{Name: "a"}, {Name: "b"}}
	got := FieldNames(fields)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected names: %v", got)
	}
}
