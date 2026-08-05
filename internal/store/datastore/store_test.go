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
