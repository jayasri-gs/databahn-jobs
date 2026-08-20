package databahnstorage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/apply"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/google/uuid"
)

const (
	testRegion  = "us-east-1"
	testStoreID = "store-1"
)

var (
	errTestClean     = errors.New("clean failed")
	errTestPreflight = errors.New("preflight failed")
)

func TestParseDatabahnStorageSearchConfig_Success(t *testing.T) {
	var sc model.SearchConfig
	sc.S3Configuration.AthenaTable = "events"
	sc.S3Configuration.DatabahnStorageRegion = testRegion
	sc.S3Configuration.S3Location = "s3://my-bucket/data/"
	cfg, err := json.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	}

	target, err := parseDatabahnStorageSearchConfig(testStoreID, uuid.New(), string(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if target.tableName != "events" || target.region != testRegion || target.outputLocation != "s3://my-bucket/athena-results/" {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestParseDatabahnStorageSearchConfig_InvalidJSON(t *testing.T) {
	_, err := parseDatabahnStorageSearchConfig(testStoreID, uuid.New(), "not-json")
	if err == nil || !strings.Contains(err.Error(), "parse search_configuration") {
		t.Fatalf("expected parse error, got %v", err)
	}
}

func TestParseDatabahnStorageSearchConfig_EmptyAthenaTable(t *testing.T) {
	_, err := parseDatabahnStorageSearchConfig(testStoreID, uuid.New(), `{}`)
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}

func TestParseDatabahnStorageSearchConfig_MissingRegion(t *testing.T) {
	var sc model.SearchConfig
	sc.S3Configuration.AthenaTable = "events"
	cfg, _ := json.Marshal(sc)
	_, err := parseDatabahnStorageSearchConfig(testStoreID, uuid.New(), string(cfg))
	if err == nil || model.IsTableNotFound(err) {
		t.Fatalf("expected region error, got %v", err)
	}
}

func TestParseDatabahnStorageSearchConfig_MissingBucket(t *testing.T) {
	var sc model.SearchConfig
	sc.S3Configuration.AthenaTable = "events"
	sc.S3Configuration.DatabahnStorageRegion = testRegion
	cfg, _ := json.Marshal(sc)
	_, err := parseDatabahnStorageSearchConfig(testStoreID, uuid.New(), string(cfg))
	if err == nil || model.IsTableNotFound(err) {
		t.Fatalf("expected bucket error, got %v", err)
	}
}

func TestProcessGroup_CleanFieldsError(t *testing.T) {
	withSyncStubs(t,
		func(ctx context.Context, destID, sourceID uuid.UUID) (databahnStorageTarget, error) {
			return databahnStorageTarget{tableName: "events", region: testRegion, outputLocation: "s3://b/out/"}, nil
		},
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, fields []model.Field) ([]model.Field, error) {
			return nil, errTestClean
		},
		nil,
	)
	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "col"}}, ptrOps(mockOps()))
	if err != errTestClean {
		t.Fatalf("expected clean error, got %v", err)
	}
}

func TestProcessGroup_PreflightError(t *testing.T) {
	withSyncStubs(t,
		func(ctx context.Context, destID, sourceID uuid.UUID) (databahnStorageTarget, error) {
			return databahnStorageTarget{tableName: "events", region: testRegion, outputLocation: "s3://b/out/"}, nil
		},
		func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, fields []model.Field) ([]model.Field, error) {
			return fields, nil
		},
		func(ctx context.Context, p apply.PreflightParams, ops apply.SchemaOps) error {
			return errTestPreflight
		},
	)
	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "col"}}, ptrOps(mockOps()))
	if err != errTestPreflight {
		t.Fatalf("expected preflight error, got %v", err)
	}
}
