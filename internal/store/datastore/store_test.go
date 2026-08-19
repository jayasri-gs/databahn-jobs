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
		want        string
	}{
		{StoreTypeDatabahnDestination, DestTypeS3, "", QueryEngineAthena},
		{StoreTypeDatabahnDestination, DestTypeAzureBlob, "", QueryEngineSynapse},
		{StoreTypeDatabahnInsights, "", "", QueryEngineAthena},
		{StoreTypeDatabahnStorage, "", "", QueryEngineAthena},
		{StoreTypeExternalStorage, "", ExternalProviderAzureBlob, QueryEngineSynapse},
		{StoreTypeExternalStorage, "", ExternalProviderS3, QueryEngineAthena},
	}
	for _, tc := range tests {
		got := DeriveQueryEngine(tc.storeType, tc.destType, tc.extProvider)
		if got != tc.want {
			t.Fatalf("DeriveQueryEngine(%q,%q,%q) = %q, want %q", tc.storeType, tc.destType, tc.extProvider, got, tc.want)
		}
	}
}
