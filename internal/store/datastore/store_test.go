package datastore

import "testing"

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
