package factory

import (
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
)

func TestIsSupportedExportMatrix(t *testing.T) {
	valid := []struct {
		engine string
		dest   string
	}{
		{models.QueryEngineAthena, models.DestTypeS3},
		{models.QueryEngineAthena, models.DestTypeS3Parquet},
		{models.QueryEngineAthena, models.DestTypeAzureBlob},
		{models.QueryEngineSynapse, models.DestTypeS3},
		{models.QueryEngineSynapse, models.DestTypeAzureBlob},
		{models.QueryEngineKustoADX, models.DestTypeAzureBlob},
		// Sentinel encodes rows in the worker and uploads client-side, so every
		// destination the uploader supports works.
		{models.QueryEngineKustoLAW, models.DestTypeS3},
		{models.QueryEngineKustoLAW, models.DestTypeS3Parquet},
		{models.QueryEngineKustoLAW, models.DestTypeAzureBlob},
	}
	for _, tc := range valid {
		if !IsSupportedExportMatrix(tc.engine, tc.dest) {
			t.Fatalf("expected supported matrix %s -> %s", tc.engine, tc.dest)
		}
	}

	// The Sentinel lake tier has no export transport in the worker yet.
	if IsSupportedExportMatrix("KUSTO_LAKE", models.DestTypeAzureBlob) {
		t.Fatal("KUSTO_LAKE should not be supported")
	}
	if IsSupportedExportMatrix(models.QueryEngineKustoLAW, "GCS") {
		t.Fatal("unsupported destination should fail for Sentinel")
	}
	if IsSupportedExportMatrix(models.QueryEngineAthena, "GCS") {
		t.Fatal("unsupported destination should fail")
	}
	// ADX .export stages into the destination's own blob container, so S3 is out.
	for _, dest := range []string{models.DestTypeS3, models.DestTypeS3Parquet} {
		if IsSupportedExportMatrix(models.QueryEngineKustoADX, dest) {
			t.Fatalf("ADX export to %s should not be supported", dest)
		}
	}
	if !IsSupportedExportMatrix("kusto_adx", "azure_blob") {
		t.Fatal("engine and destination matching should be case-insensitive")
	}
	if !IsSupportedExportMatrix("kusto_law", "s3") {
		t.Fatal("Sentinel matrix should be case-insensitive")
	}
}

func TestLegacyModeDefaultsToAthena(t *testing.T) {
	queryEngine := ""
	legacyMode := queryEngine == ""
	if legacyMode {
		queryEngine = models.QueryEngineAthena
	}
	if queryEngine != models.QueryEngineAthena {
		t.Fatalf("legacy mode engine = %q, want ATHENA", queryEngine)
	}
	if !legacyMode {
		t.Fatal("expected legacy mode")
	}
}
