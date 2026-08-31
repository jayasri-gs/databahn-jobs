package datastore

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const (
	testExternalAthenaRegion = "us-east-1"
	testExternalAthenaBucket = "external-bucket"
)

func TestResolveExternalAthenaS3_UsesConnectorConfig(t *testing.T) {
	storeCfg := dataStoreConfiguration{
		ExternalSearchDataStoreConfiguration: &externalStoreConfigJSON{
			ExternalSearchProvider: ExternalProviderS3,
			ConnectorConfig: map[string]string{
				"region":            testExternalAthenaRegion,
				"bucket":            testExternalAthenaBucket,
				"auth_type":         "key_based",
				"access_key_id":     "AKIAEXAMPLE",
				"secret_access_key": "shh",
			},
		},
	}

	cfg, err := resolveExternalAthenaS3(context.Background(), nil, uuid.Nil, uuid.Nil, storeCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Region != testExternalAthenaRegion {
		t.Fatalf("region = %q, want %q", cfg.Region, testExternalAthenaRegion)
	}
	if cfg.Bucket != testExternalAthenaBucket {
		t.Fatalf("bucket = %q, want %q", cfg.Bucket, testExternalAthenaBucket)
	}
	if cfg.AuthType != "key_based" || cfg.AccessKeyID != "AKIAEXAMPLE" || cfg.SecretAccessKey != "shh" {
		t.Fatalf("unexpected creds: %+v", cfg)
	}
}

func TestResolveExternalAthenaS3_MissingRegion(t *testing.T) {
	storeCfg := dataStoreConfiguration{
		ExternalSearchDataStoreConfiguration: &externalStoreConfigJSON{
			ExternalSearchProvider: ExternalProviderS3,
			ConnectorConfig:        map[string]string{"bucket": testExternalAthenaBucket},
		},
	}

	_, err := resolveExternalAthenaS3(context.Background(), nil, uuid.Nil, uuid.Nil, storeCfg)
	if err == nil || !strings.Contains(err.Error(), "region not configured") {
		t.Fatalf("expected region error, got %v", err)
	}
}

func TestResolveExternalAthenaS3_MissingBucket(t *testing.T) {
	storeCfg := dataStoreConfiguration{
		ExternalSearchDataStoreConfiguration: &externalStoreConfigJSON{
			ExternalSearchProvider: ExternalProviderS3,
			ConnectorConfig:        map[string]string{"region": testExternalAthenaRegion},
		},
	}

	_, err := resolveExternalAthenaS3(context.Background(), nil, uuid.Nil, uuid.Nil, storeCfg)
	if err == nil || !strings.Contains(err.Error(), "bucket not configured") {
		t.Fatalf("expected bucket error, got %v", err)
	}
}

func TestResolveExternalAthenaS3_KeyBasedMissingCredentials(t *testing.T) {
	storeCfg := dataStoreConfiguration{
		ExternalSearchDataStoreConfiguration: &externalStoreConfigJSON{
			ExternalSearchProvider: ExternalProviderS3,
			ConnectorConfig: map[string]string{
				"region":    testExternalAthenaRegion,
				"bucket":    testExternalAthenaBucket,
				"auth_type": "key_based",
			},
		},
	}

	_, err := resolveExternalAthenaS3(context.Background(), nil, uuid.Nil, uuid.Nil, storeCfg)
	if err == nil || !strings.Contains(err.Error(), "access_key_id/secret_access_key not configured") {
		t.Fatalf("expected key_based credentials error, got %v", err)
	}
}

func TestResolveExternalAthenaS3_RoleBasedMissingRoleArn(t *testing.T) {
	storeCfg := dataStoreConfiguration{
		ExternalSearchDataStoreConfiguration: &externalStoreConfigJSON{
			ExternalSearchProvider: ExternalProviderS3,
			ConnectorConfig: map[string]string{
				"region":    testExternalAthenaRegion,
				"bucket":    testExternalAthenaBucket,
				"auth_type": "role_based",
			},
		},
	}

	_, err := resolveExternalAthenaS3(context.Background(), nil, uuid.Nil, uuid.Nil, storeCfg)
	if err == nil || !strings.Contains(err.Error(), "role_arn not configured") {
		t.Fatalf("expected role_based role_arn error, got %v", err)
	}
}

func TestResolveExternalAthenaS3_UnsupportedAuthType(t *testing.T) {
	storeCfg := dataStoreConfiguration{
		ExternalSearchDataStoreConfiguration: &externalStoreConfigJSON{
			ExternalSearchProvider: ExternalProviderS3,
			ConnectorConfig: map[string]string{
				"region": testExternalAthenaRegion,
				"bucket": testExternalAthenaBucket,
			},
		},
	}

	_, err := resolveExternalAthenaS3(context.Background(), nil, uuid.Nil, uuid.Nil, storeCfg)
	if err == nil || !strings.Contains(err.Error(), "unsupported or missing auth_type") {
		t.Fatalf("expected unsupported auth_type error, got %v", err)
	}
}

func TestResolveExternalAthenaS3_NilExternalConfig(t *testing.T) {
	_, err := resolveExternalAthenaS3(context.Background(), nil, uuid.Nil, uuid.Nil, dataStoreConfiguration{})
	if err == nil || !strings.Contains(err.Error(), "external search data store configuration is required") {
		t.Fatalf("expected config-required error, got %v", err)
	}
}

func TestDeriveQueryEngine(t *testing.T) {
	tests := []struct {
		storeType   string
		destType    string
		extProvider string
		storageTier string
		want        string
	}{
		{StoreTypeDatabahnDestination, DestTypeS3, "", "", QueryEngineAthena},
		{StoreTypeDatabahnDestination, DestTypeAWSSecurityLake, "", "", QueryEngineAthena},
		{StoreTypeDatabahnDestination, DestTypeAzureBlob, "", "", QueryEngineSynapse},
		{StoreTypeDatabahnInsights, "", "", "", QueryEngineAthena},
		{StoreTypeDatabahnStorage, "", "", "", QueryEngineAthena},
		{StoreTypeExternalStorage, "", ExternalProviderAzureBlob, "", QueryEngineSynapse},
		{StoreTypeExternalStorage, "", ExternalProviderS3, "", QueryEngineAthena},
		{StoreTypeExternalStorage, "", ExternalProviderSecurityLake, "", QueryEngineAthena},
		{StoreTypeDerivedDatastore, "", ExternalProviderSecurityLake, "", QueryEngineAthena},
		{StoreTypeExternalStorage, "", ExternalProviderADX, "", QueryEngineKustoADX},
		{StoreTypeDerivedDatastore, "", ExternalProviderADX, "", QueryEngineKustoADX},
		{StoreTypeDatabahnDestination, DestTypeAzureBlob, ExternalProviderADX, "", QueryEngineSynapse},
		{StoreTypeDatabahnDestination, DestTypeAzureDataExplorer, "", "", QueryEngineKustoADX},
		// storage_tier separates the two Sentinel engines: they use different endpoints,
		// Entra scopes and response formats. A blank tier means analytics.
		{StoreTypeExternalStorage, "", ExternalProviderSentinel, "", QueryEngineKustoLAW},
		{StoreTypeExternalStorage, "", ExternalProviderSentinel, "ANALYTICS", QueryEngineKustoLAW},
		{StoreTypeDerivedDatastore, "", ExternalProviderSentinel, "", QueryEngineKustoLAW},
		{StoreTypeExternalStorage, "", ExternalProviderSentinel, "LAKE", QueryEngineKustoLake},
		{StoreTypeDerivedDatastore, "", ExternalProviderSentinel, "lake", QueryEngineKustoLake},
	}
	for _, tc := range tests {
		got := DeriveQueryEngine(tc.storeType, tc.destType, tc.extProvider, tc.storageTier)
		if got != tc.want {
			t.Fatalf("DeriveQueryEngine(%q,%q,%q,%q) = %q, want %q", tc.storeType, tc.destType, tc.extProvider, tc.storageTier, got, tc.want)
		}
	}
}

func TestIsExternalAthenaProvider(t *testing.T) {
	if !IsExternalAthenaProvider(ExternalProviderSecurityLake, nil) {
		t.Fatal("SECURITY_LAKE should be external Athena")
	}
	if !IsExternalAthenaProvider(ExternalProviderS3, map[string]string{"security_lake": "true"}) {
		t.Fatal("legacy security_lake flag should be external Athena")
	}
	if IsExternalAthenaProvider(ExternalProviderAzureBlob, nil) {
		t.Fatal("AZURE_BLOB should not be external Athena")
	}
}

func TestIsExternalAthenaProvider_ADX(t *testing.T) {
	if IsExternalAthenaProvider(ExternalProviderADX, nil) {
		t.Fatal("AZURE_DATA_EXPLORER must not be treated as external Athena")
	}
}

// LoadExportDataStore loads Sentinel workspace credentials behind IsSentinelEngine. If the two
// ever disagree, a Sentinel store reaches the export factory with a nil Sentinel config and the
// job fails with "missing workspace credentials" — which is what happened when the load was
// keyed on KUSTO_LAW alone and the lake tier was added.
func TestIsSentinelEngineCoversEveryEngineDerivedForSentinel(t *testing.T) {
	for _, tier := range []string{"", "ANALYTICS", "analytics", "LAKE", "lake"} {
		for _, storeType := range []string{StoreTypeExternalStorage, StoreTypeDerivedDatastore} {
			engine := DeriveQueryEngine(storeType, "", ExternalProviderSentinel, tier)
			if !IsSentinelEngine(engine) {
				t.Fatalf(
					"DeriveQueryEngine(%q, provider=SENTINEL, tier=%q) = %q, which IsSentinelEngine rejects",
					storeType, tier, engine)
			}
		}
	}
}

func TestIsSentinelEngineRejectsOtherEngines(t *testing.T) {
	for _, engine := range []string{QueryEngineAthena, QueryEngineSynapse, QueryEngineKustoADX, "", "SPL"} {
		if IsSentinelEngine(engine) {
			t.Fatalf("IsSentinelEngine(%q) = true", engine)
		}
	}
}
