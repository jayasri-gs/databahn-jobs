package model

import (
	"errors"
	"testing"

	athenastore "github.com/databahn-ai/databahn-jobs/internal/store/athena"
	"github.com/google/uuid"
)

func TestIsTableNotFound_TypedError(t *testing.T) {
	err := NewTableNotFoundError("missing table")
	if !IsTableNotFound(err) {
		t.Fatal("expected TableNotFoundError to match")
	}
}

func TestIsTableNotFound_AthenaPackageError(t *testing.T) {
	err := &athenastore.TableNotFoundError{Msg: "TABLE_NOT_FOUND"}
	if !IsTableNotFound(err) {
		t.Fatal("expected athena TableNotFoundError to match")
	}
}

func TestIsTableNotFound_MessageHeuristic(t *testing.T) {
	if !IsTableNotFound(errors.New("Table not found: db.tbl")) {
		t.Fatal("expected message heuristic to match")
	}
	if IsTableNotFound(errors.New("connection refused")) {
		t.Fatal("expected unrelated error to not match")
	}
}

func TestBucketFromS3Location(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"s3://my-bucket/path/to/data/", "my-bucket"},
		{"s3://bucket-only", "bucket-only"},
		{"s3://b", "b"},
	}
	for _, tc := range cases {
		if got := BucketFromS3Location(tc.in); got != tc.want {
			t.Fatalf("BucketFromS3Location(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestDatabaseName(t *testing.T) {
	tenantID := uuid.MustParse("dbd00000-0000-0000-0000-000000000001")
	got := DatabaseName(tenantID)
	want := "databahn_tenant_dbd00000_0000_0000_0000_000000000001"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
