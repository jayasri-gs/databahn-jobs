package datastore

import "testing"

func TestDeriveQueryEngine(t *testing.T) {
	tests := []struct {
		storeType   string
		destType    string
		extProvider string
		want        string
	}{
		{StoreTypeDatabahnDestination, DestTypeS3, "", QueryEngineAthena},
		{StoreTypeDatabahnDestination, DestTypeAWSSecurityLake, "", QueryEngineAthena},
		{StoreTypeDatabahnDestination, DestTypeAzureBlob, "", QueryEngineSynapse},
		{StoreTypeDatabahnInsights, "", "", QueryEngineAthena},
		{StoreTypeExternalStorage, "", ExternalProviderAzureBlob, QueryEngineSynapse},
		{StoreTypeExternalStorage, "", ExternalProviderS3, QueryEngineAthena},
		{StoreTypeExternalStorage, "", ExternalProviderSecurityLake, QueryEngineAthena},
		{StoreTypeDerivedDatastore, "", ExternalProviderSecurityLake, QueryEngineAthena},
	}
	for _, tc := range tests {
		got := DeriveQueryEngine(tc.storeType, tc.destType, tc.extProvider)
		if got != tc.want {
			t.Fatalf("DeriveQueryEngine(%q,%q,%q) = %q, want %q", tc.storeType, tc.destType, tc.extProvider, got, tc.want)
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
