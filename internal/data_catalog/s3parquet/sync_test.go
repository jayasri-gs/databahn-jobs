package s3parquet

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/apply"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/google/uuid"
)

func testIDs() (destID, sourceID, tenantID uuid.UUID) {
	return uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		uuid.MustParse("33333333-3333-3333-3333-333333333333")
}

func destinationConfigJSON(region, bucket string) string {
	wrapper := destinationConfigWrapper{
		Configuration: map[string]interface{}{
			"auth_type":         "key_based",
			"access_key_id":     "AKIATEST",
			"secret_access_key": "secret",
			"region":            region,
			"bucket":            bucket,
		},
	}
	b, _ := json.Marshal(wrapper)
	return string(b)
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

func TestDestinationConfigWrapper_getString(t *testing.T) {
	w := destinationConfigWrapper{
		Configuration: map[string]interface{}{
			"region": testRegionEast,
			"count":  42,
		},
	}
	if w.getString("region") != testRegionEast {
		t.Fatal("expected region string")
	}
	if w.getString("count") != "42" {
		t.Fatal("expected formatted non-string value")
	}
	if w.getString("missing") != "" {
		t.Fatal("expected empty for missing key")
	}
}

func TestApplyS3ParquetCatalogToAthena_NoFields(t *testing.T) {
	oldQuery := queryUnappliedFields
	queryUnappliedFields = func(ctx context.Context) ([]model.Field, error) { return nil, nil }
	t.Cleanup(func() { queryUnappliedFields = oldQuery })

	result := ApplyS3ParquetCatalogToAthena(context.Background())
	if len(result.Errors) != 0 {
		t.Fatalf("expected success, got %v", result.Errors)
	}
}

func TestApplyS3ParquetCatalogToAthena_SkipsTableNotFound(t *testing.T) {
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

	result := ApplyS3ParquetCatalogToAthena(context.Background())
	if len(result.Errors) != 0 {
		t.Fatalf("expected success with skip, got %v", result.Errors)
	}
}

func TestProcessGroup_SuccessWithMockOps(t *testing.T) {
	oldResolve := resolveS3ParquetTarget
	oldClean := cleanFields
	oldPreflight := runPreflight
	resolveS3ParquetTarget = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID) (s3ParquetTarget, error) {
		return s3ParquetTarget{
			tableName:      "events",
			outputLocation: testOutputLocation,
			destConfig:     &destinationConfig{AuthType: "key_based", Region: testRegionWest, Bucket: "cust"},
		}, nil
	}
	cleanFields = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, fields []model.Field) ([]model.Field, error) {
		return fields, nil
	}
	runPreflight = func(ctx context.Context, p apply.PreflightParams, ops apply.SchemaOps) error {
		return nil
	}
	t.Cleanup(func() {
		resolveS3ParquetTarget = oldResolve
		cleanFields = oldClean
		runPreflight = oldPreflight
	})

	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "new_col"}}, ptrOps(mockOps()))
	if err != nil {
		t.Fatal(err)
	}
}

func TestProcessGroup_ResolveError(t *testing.T) {
	oldResolve := resolveS3ParquetTarget
	resolveS3ParquetTarget = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID) (s3ParquetTarget, error) {
		return s3ParquetTarget{}, model.NewTableNotFoundError("missing store")
	}
	t.Cleanup(func() { resolveS3ParquetTarget = oldResolve })

	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "col"}}, ptrOps(mockOps()))
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}

func TestProcessGroup_CreateClientError(t *testing.T) {
	oldResolve := resolveS3ParquetTarget
	oldClean := cleanFields
	oldCreate := createAthenaClient
	resolveS3ParquetTarget = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID) (s3ParquetTarget, error) {
		return s3ParquetTarget{
			tableName:      "events",
			outputLocation: testOutputLocation,
			destConfig:     &destinationConfig{AuthType: "key_based", Region: testRegionWest},
		}, nil
	}
	cleanFields = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, fields []model.Field) ([]model.Field, error) {
		return fields, nil
	}
	createAthenaClient = func(ctx context.Context, cfg *destinationConfig) (*athena.Client, error) {
		return nil, errors.New("client failed")
	}
	t.Cleanup(func() {
		resolveS3ParquetTarget = oldResolve
		cleanFields = oldClean
		createAthenaClient = oldCreate
	})

	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "col"}}, nil)
	if err == nil {
		t.Fatal("expected client error")
	}
}

func TestParseDestinationWrapperConfiguration_InvalidJSON(t *testing.T) {
	_, err := parseDestinationWrapperConfiguration("not-json")
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestParseDestinationWrapperConfiguration_Success(t *testing.T) {
	parsed, err := parseDestinationWrapperConfiguration(destinationConfigJSON("eu-west-1", "cust-bucket"))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.config.Region != "eu-west-1" || parsed.config.Bucket != "cust-bucket" {
		t.Fatalf("unexpected config: %+v", parsed.config)
	}
}

func TestParseS3ParquetTableName_Success(t *testing.T) {
	var sc model.SearchConfig
	sc.S3Configuration.AthenaTable = "events"
	cfg, _ := json.Marshal(sc)
	name, err := parseS3ParquetTableName(testStoreID, uuid.New(), string(cfg))
	if err != nil || name != "events" {
		t.Fatalf("unexpected result: name=%q err=%v", name, err)
	}
}

func TestParseS3ParquetTableName_EmptyTable(t *testing.T) {
	_, err := parseS3ParquetTableName(testStoreID, uuid.New(), `{}`)
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}

func TestParseS3ParquetTableName_InvalidJSON(t *testing.T) {
	_, err := parseS3ParquetTableName(testStoreID, uuid.New(), "bad")
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestCreateCustomerAthenaClient_StaticCredentials(t *testing.T) {
	client, err := createCustomerAthenaClient(context.Background(), &destinationConfig{
		AuthType:    "key_based",
		AccessKeyID: "AKIATEST",
		SecretKey:   "secret",
		Region:      testRegionEast,
	})
	if err != nil || client == nil {
		t.Fatalf("expected client, got err=%v client=%v", err, client)
	}
}

func TestCreateCustomerAthenaClient_RoleBased(t *testing.T) {
	client, err := createCustomerAthenaClient(context.Background(), &destinationConfig{
		AuthType:   "role_based",
		RoleArn:    "arn:aws:iam::123456789012:role/TestRole",
		ExternalID: "ext-123",
		Region:     testRegionEast,
	})
	if err != nil || client == nil {
		t.Fatalf("expected client, got err=%v client=%v", err, client)
	}
}

func TestProcessGroup_PreflightError(t *testing.T) {
	oldResolve := resolveS3ParquetTarget
	oldClean := cleanFields
	oldPreflight := runPreflight
	resolveS3ParquetTarget = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID) (s3ParquetTarget, error) {
		return s3ParquetTarget{
			tableName:      "events",
			outputLocation: testOutputLocation,
			destConfig:     &destinationConfig{Region: testRegionWest},
		}, nil
	}
	cleanFields = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, dispenserType string, fields []model.Field) ([]model.Field, error) {
		return fields, nil
	}
	runPreflight = func(ctx context.Context, p apply.PreflightParams, ops apply.SchemaOps) error {
		return errors.New("preflight failed")
	}
	t.Cleanup(func() {
		resolveS3ParquetTarget = oldResolve
		cleanFields = oldClean
		runPreflight = oldPreflight
	})

	destID, sourceID, tenantID := testIDs()
	err := processGroup(context.Background(), destID, sourceID, tenantID,
		[]model.Field{{ID: 1, Name: "col"}}, ptrOps(mockOps()))
	if err == nil {
		t.Fatal("expected preflight error")
	}
}
