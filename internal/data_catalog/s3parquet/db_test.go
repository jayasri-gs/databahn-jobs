package s3parquet

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/apply"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/dbtest"
	"github.com/google/uuid"
)

const (
	testStoreID        = "store-1"
	testBucketA        = "bucket-a"
	testRegionEast     = "us-east-1"
	testRegionWest     = "us-west-2"
	testOutputLocation = "s3://cust/.databahn_out/"
)

func useDefaultDBDeps(t *testing.T) {
	t.Helper()
	oldQuery := queryUnappliedFields
	oldResolve := resolveS3ParquetTarget
	oldGet := getDestConfig
	queryUnappliedFields = defaultQueryUnappliedS3ParquetFields
	resolveS3ParquetTarget = defaultResolveS3ParquetTarget
	getDestConfig = getDestinationConfig
	t.Cleanup(func() {
		queryUnappliedFields = oldQuery
		resolveS3ParquetTarget = oldResolve
		getDestConfig = oldGet
	})
}

func TestDefaultQueryUnappliedS3ParquetFields_WithMockDB(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, sourceID, tenantID := testIDs()

	mock.ExpectQuery(`SELECT .* FROM "data_catalog"`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "field_type", "source_id", "destination_id", "tenant_id"}).
			AddRow(1, "col", "string", sourceID, destID, tenantID))

	fields, err := queryUnappliedFields(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 {
		t.Fatalf("unexpected fields: %v", fields)
	}
}

func TestDefaultResolveS3ParquetTarget_WithMockDB(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, sourceID, tenantID := testIDs()

	var sc model.SearchConfig
	sc.S3Configuration.AthenaTable = "events"
	cfg, _ := json.Marshal(sc)

	mock.ExpectQuery(`SELECT id FROM search_data_store`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(testStoreID))
	mock.ExpectQuery(`SELECT search_configuration FROM search_data_set`).
		WillReturnRows(sqlmock.NewRows([]string{"search_configuration"}).AddRow(string(cfg)))
	mock.ExpectQuery(`SELECT id, tenant_id, configuration FROM destination`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "configuration"}).
			AddRow(destID, tenantID, destinationConfigJSON("us-west-2", "cust-bucket")))

	target, err := resolveS3ParquetTarget(context.Background(), destID, sourceID, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if target.tableName != "events" || target.destConfig.Region != "us-west-2" {
		t.Fatalf("unexpected target: %+v", target)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetDestinationConfig_WithMockDB(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, _, tenantID := testIDs()

	mock.ExpectQuery(`SELECT id, tenant_id, configuration FROM destination`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "configuration"}).
			AddRow(destID, tenantID, destinationConfigJSON("eu-west-1", testBucketA)))

	cfg, err := getDestinationConfig(context.Background(), destID, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region != "eu-west-1" || cfg.Bucket != testBucketA {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestDefaultResolveS3ParquetTarget_StoreNotFound(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, sourceID, tenantID := testIDs()

	mock.ExpectQuery(`SELECT id FROM search_data_store`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	_, err := resolveS3ParquetTarget(context.Background(), destID, sourceID, tenantID)
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}

func TestDefaultResolveS3ParquetTarget_DatasetNotFound(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, sourceID, tenantID := testIDs()

	mock.ExpectQuery(`SELECT id FROM search_data_store`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(testStoreID))
	mock.ExpectQuery(`SELECT search_configuration FROM search_data_set`).
		WillReturnRows(sqlmock.NewRows([]string{"search_configuration"}))

	_, err := resolveS3ParquetTarget(context.Background(), destID, sourceID, tenantID)
	if !model.IsTableNotFound(err) {
		t.Fatalf("expected table not found, got %v", err)
	}
}

func TestDefaultResolveS3ParquetTarget_EmptyRegion(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, sourceID, tenantID := testIDs()

	var sc model.SearchConfig
	sc.S3Configuration.AthenaTable = "events"
	cfg, _ := json.Marshal(sc)

	mock.ExpectQuery(`SELECT id FROM search_data_store`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(testStoreID))
	mock.ExpectQuery(`SELECT search_configuration FROM search_data_set`).
		WillReturnRows(sqlmock.NewRows([]string{"search_configuration"}).AddRow(string(cfg)))
	mock.ExpectQuery(`SELECT id, tenant_id, configuration FROM destination`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "configuration"}).
			AddRow(destID, tenantID, destinationConfigJSON("", "cust-bucket")))

	_, err := resolveS3ParquetTarget(context.Background(), destID, sourceID, tenantID)
	if err == nil || model.IsTableNotFound(err) {
		t.Fatalf("expected region error, got %v", err)
	}
}

func TestApplyS3ParquetCatalogToAthena_CollectsErrors(t *testing.T) {
	destID, sourceID, tenantID := testIDs()
	oldQuery := queryUnappliedFields
	oldProcess := processGroupFn
	queryUnappliedFields = func(ctx context.Context) ([]model.Field, error) {
		return []model.Field{{ID: 1, Name: "col", DestID: destID, SourceID: sourceID, TenantID: tenantID}}, nil
	}
	processGroupFn = func(ctx context.Context, destID, sourceID, tenantID uuid.UUID, fields []model.Field, ops *apply.SchemaOps) error {
		return errors.New("boom")
	}
	t.Cleanup(func() {
		queryUnappliedFields = oldQuery
		processGroupFn = oldProcess
	})

	result := ApplyS3ParquetCatalogToAthena(context.Background())
	if len(result.Errors) != 1 {
		t.Fatalf("expected one error, got %v", result.Errors)
	}
}

func TestGetDestinationConfig_EmptyConfiguration(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, _, tenantID := testIDs()

	mock.ExpectQuery(`SELECT id, tenant_id, configuration FROM destination`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "configuration"}).
			AddRow(destID, tenantID, ""))

	_, err := getDestinationConfig(context.Background(), destID, tenantID)
	if err == nil {
		t.Fatal("expected empty configuration error")
	}
}

func TestGetDestinationConfig_NotFound(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, _, tenantID := testIDs()

	mock.ExpectQuery(`SELECT id, tenant_id, configuration FROM destination`).
		WillReturnError(sqlmock.ErrCancelled)

	_, err := getDestinationConfig(context.Background(), destID, tenantID)
	if err == nil {
		t.Fatal("expected not found error")
	}
}

func destinationConfigWithSecretJSON(secretID, region, bucket string) string {
	wrapper := destinationConfigWrapper{
		SecretID: secretID,
		Configuration: map[string]interface{}{
			"region": region,
			"bucket": bucket,
		},
	}
	b, _ := json.Marshal(wrapper)
	return string(b)
}

func TestGetDestinationConfig_SecretLookupFailure(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	destID, _, tenantID := testIDs()

	mock.ExpectQuery(`SELECT id, tenant_id, configuration FROM destination`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "configuration"}).
			AddRow(destID, tenantID, destinationConfigWithSecretJSON("secret-ref", testRegionEast, testBucketA)))
	mock.ExpectQuery(`SELECT backend_secret_id FROM secrets`).
		WillReturnRows(sqlmock.NewRows([]string{"backend_secret_id"}))

	_, err := getDestinationConfig(context.Background(), destID, tenantID)
	if err == nil {
		t.Fatal("expected secret lookup error")
	}
}

func TestGetDestinationConfig_SecretMergeSuccess(t *testing.T) {
	useDefaultDBDeps(t)
	_, mock := dbtest.MockPostgres(t)

	oldRead := readSecretByName
	oldRegion := appRegion
	readSecretByName = func(name, region string) (*secretsmanager.GetSecretValueOutput, error) {
		secret := `{"access_key_id":"AKIA","secret_access_key":"SECRET","role_arn":"arn:aws:iam::1:role/R"}`
		return &secretsmanager.GetSecretValueOutput{SecretString: &secret}, nil
	}
	appRegion = func() string { return testRegionEast }
	t.Cleanup(func() {
		readSecretByName = oldRead
		appRegion = oldRegion
	})

	destID, _, tenantID := testIDs()

	mock.ExpectQuery(`SELECT id, tenant_id, configuration FROM destination`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "configuration"}).
			AddRow(destID, tenantID, destinationConfigWithSecretJSON("secret-ref", testRegionEast, testBucketA)))
	mock.ExpectQuery(`SELECT backend_secret_id FROM secrets`).
		WillReturnRows(sqlmock.NewRows([]string{"backend_secret_id"}).AddRow("backend-secret"))

	cfg, err := getDestinationConfig(context.Background(), destID, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccessKeyID != "AKIA" || cfg.RoleArn == "" {
		t.Fatalf("expected merged secret credentials, got %+v", cfg)
	}
}
