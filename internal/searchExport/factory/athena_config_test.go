package factory

import (
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

func TestResolveAthenaClientConfig_prefersExportConfigOverrides(t *testing.T) {
	staging := &destination.S3Config{
		AuthType:   "role_based",
		RoleArn:    "arn:aws:iam::123:role/store",
		ExternalID: "ext",
		Region:     "eu-west-1",
		Bucket:     "sl-athena-out",
	}
	cfg := &models.SearchExportConfig{
		ExternalSearchProvider: "SECURITY_LAKE",
		Athena: &models.AthenaExportConfig{
			Region:         "us-east-1",
			OutputLocation: "s3://sl-athena-out/.databahn_out",
		},
	}

	got, err := resolveAthenaClientConfig(cfg, staging)
	if err != nil {
		t.Fatalf("resolveAthenaClientConfig: %v", err)
	}
	if got.Region != "us-east-1" {
		t.Fatalf("region = %q, want us-east-1", got.Region)
	}
	// The output location is derived from staging.Bucket, never from the export config —
	// see athenaOutputLocationFor. The config value below is ignored.
	if got.OutputLocation != "s3://sl-athena-out/.databahn_out" {
		t.Fatalf("outputLocation = %q", got.OutputLocation)
	}
	if got.AuthType != "role_based" || got.RoleArn != staging.RoleArn {
		t.Fatalf("credentials not taken from staging: %+v", got)
	}
}

func TestResolveAthenaClientConfig_fallsBackToStaging(t *testing.T) {
	staging := &destination.S3Config{
		AuthType:        "key_based",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		Region:          "ap-south-1",
		Bucket:          "store-bucket",
	}

	got, err := resolveAthenaClientConfig(&models.SearchExportConfig{}, staging)
	if err != nil {
		t.Fatalf("resolveAthenaClientConfig: %v", err)
	}
	if got.Region != "ap-south-1" {
		t.Fatalf("region = %q", got.Region)
	}
	if got.OutputLocation != "s3://store-bucket/.databahn_out" {
		t.Fatalf("outputLocation = %q", got.OutputLocation)
	}
}

// Security Lake is the only Athena source whose catalog exposes microsecond timestamps, so it
// is the only one that opts into the UNLOAD timestamp rewrite.
func TestResolveAthenaClientConfig_narrowsTimestampsForSecurityLakeOnly(t *testing.T) {
	staging := &destination.S3Config{
		AuthType: "role_based", RoleArn: "arn:aws:iam::123:role/store",
		Region: "eu-north-1", Bucket: "sl-athena-out",
	}
	tests := map[string]bool{
		"SECURITY_LAKE": true,
		"security_lake": true,
		"S3":            false,
		"":              false,
	}
	for provider, want := range tests {
		got, err := resolveAthenaClientConfig(
			&models.SearchExportConfig{ExternalSearchProvider: provider}, staging)
		if err != nil {
			t.Fatalf("resolveAthenaClientConfig(%q): %v", provider, err)
		}
		if got.NarrowTimestamps != want {
			t.Fatalf("provider %q: NarrowTimestamps = %v, want %v", provider, got.NarrowTimestamps, want)
		}
	}
}
