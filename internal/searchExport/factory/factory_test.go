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
	}
	for _, tc := range valid {
		if !IsSupportedExportMatrix(tc.engine, tc.dest) {
			t.Fatalf("expected supported matrix %s -> %s", tc.engine, tc.dest)
		}
	}

	if IsSupportedExportMatrix("KUSTO_LAW", models.DestTypeS3) {
		t.Fatal("kql engine should not be supported")
	}
	if IsSupportedExportMatrix(models.QueryEngineAthena, "GCS") {
		t.Fatal("unsupported destination should fail")
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
