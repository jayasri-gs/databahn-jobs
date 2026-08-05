package datastore

import (
	"testing"
)

func TestParseInsightsStagingS3Config_Valid(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secret-value",
		Bucket:          "platform-insights-bucket",
	}
	cfg, err := parseInsightsStagingS3Config(secret, "us-east-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Region != "us-east-1" {
		t.Fatalf("region: got %q, want %q", cfg.Region, "us-east-1")
	}
	wantBucket := "platform-insights-bucket-parquet"
	if cfg.Bucket != wantBucket {
		t.Fatalf("bucket: got %q, want %q", cfg.Bucket, wantBucket)
	}
	if cfg.AuthType != "key_based" {
		t.Fatalf("auth_type: got %q, want %q", cfg.AuthType, "key_based")
	}
	if cfg.AccessKeyID != "AKIATEST" {
		t.Fatal("AccessKeyID mismatch")
	}
	if cfg.SecretAccessKey != "secret-value" {
		t.Fatal("SecretAccessKey mismatch")
	}
}

func TestParseInsightsStagingS3Config_NoRoleArn(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secret-value",
		Bucket:          "platform-insights-bucket",
	}
	cfg, err := parseInsightsStagingS3Config(secret, "us-east-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RoleArn != "" {
		t.Fatalf("RoleArn must be empty, got %q", cfg.RoleArn)
	}
}

func TestParseInsightsStagingS3Config_MissingBucket(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secret-value",
	}
	_, err := parseInsightsStagingS3Config(secret, "us-east-1")
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestParseInsightsStagingS3Config_MissingRegion(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secret-value",
		Bucket:          "platform-insights-bucket",
	}
	_, err := parseInsightsStagingS3Config(secret, "")
	if err == nil {
		t.Fatal("expected error for missing region")
	}
}

func TestParseInsightsStagingS3Config_MissingAccessKeyID(t *testing.T) {
	secret := insightsAwsSecret{
		SecretAccessKey: "secret-value",
		Bucket:          "platform-insights-bucket",
	}
	_, err := parseInsightsStagingS3Config(secret, "us-east-1")
	if err == nil {
		t.Fatal("expected error for missing AccessKeyID")
	}
}

func TestParseInsightsStagingS3Config_MissingSecretAccessKey(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID: "AKIATEST",
		Bucket:      "platform-insights-bucket",
	}
	_, err := parseInsightsStagingS3Config(secret, "us-east-1")
	if err == nil {
		t.Fatal("expected error for missing SecretAccessKey")
	}
}
