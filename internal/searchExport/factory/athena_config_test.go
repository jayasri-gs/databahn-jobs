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
		Bucket:     "fallback-bucket",
	}
	cfg := &models.SearchExportConfig{
		Region:                 "us-east-1",
		AthenaOutputLocation:   "s3://sl-athena-out/.databahn_out",
		ExternalSearchProvider: "SECURITY_LAKE",
	}

	got, err := resolveAthenaClientConfig(cfg, staging)
	if err != nil {
		t.Fatalf("resolveAthenaClientConfig: %v", err)
	}
	if got.Region != "us-east-1" {
		t.Fatalf("region = %q, want us-east-1", got.Region)
	}
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
