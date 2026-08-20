package models

import (
	"encoding/json"
	"testing"
)

func TestSearchExportConfig_deserializesSecurityLakeFields(t *testing.T) {
	raw := `{
		"searchExportConfig": {
			"query": "SELECT * FROM t WHERE eventDay BETWEEN '20231114' AND '20231115' LIMIT 5000",
			"queryEngine": "ATHENA",
			"database": "amazon_security_lake_glue_db_us_east_1",
			"tableName": "amazon_security_lake_table_us_east_1_vpc_flow_2_0",
			"dataStoreId": "11111111-1111-1111-1111-111111111111",
			"dataSetId": "22222222-2222-2222-2222-222222222222",
			"dataStoreType": "EXTERNAL_STORAGE",
			"format": "csv",
			"destinationId": "33333333-3333-3333-3333-333333333333",
			"destinationType": "S3_PARQUET",
			"externalSearchProvider": "SECURITY_LAKE",
			"athenaOutputLocation": "s3://sl-athena-out/.databahn_out",
			"region": "us-east-1"
		}
	}`

	var report SearchExportReport
	report.ReportConfiguration = []byte(raw)

	cfg, err := report.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if cfg.ExternalSearchProvider != "SECURITY_LAKE" {
		t.Fatalf("externalSearchProvider = %q", cfg.ExternalSearchProvider)
	}
	if cfg.AthenaOutputLocation != "s3://sl-athena-out/.databahn_out" {
		t.Fatalf("athenaOutputLocation = %q", cfg.AthenaOutputLocation)
	}
	if cfg.Region != "us-east-1" {
		t.Fatalf("region = %q", cfg.Region)
	}
}

func TestSearchExportConfig_omitsSecurityLakeFieldsForLegacyRows(t *testing.T) {
	var cfg SearchExportConfig
	if err := json.Unmarshal([]byte(`{"query":"SELECT 1","database":"db"}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.ExternalSearchProvider != "" || cfg.AthenaOutputLocation != "" || cfg.Region != "" {
		t.Fatalf("expected empty external Athena fields, got %+v", cfg)
	}
}
