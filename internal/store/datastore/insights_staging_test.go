package datastore

import (
	"testing"
)

const (
	testInsightsSecretAccessKey = "secret-value"
	testInsightsBucket          = "platform-insights-bucket"
	testInsightsRegion          = "us-east-1"
)

func TestParseInsightsStagingS3Config_Valid(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: testInsightsSecretAccessKey,
		Bucket:          testInsightsBucket,
	}
	cfg, err := parseInsightsStagingS3Config(secret, testInsightsRegion)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Region != testInsightsRegion {
		t.Fatalf("region: got %q, want %q", cfg.Region, testInsightsRegion)
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
	if cfg.SecretAccessKey != testInsightsSecretAccessKey {
		t.Fatal("SecretAccessKey mismatch")
	}
}

func TestParseInsightsStagingS3Config_NoRoleArn(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: testInsightsSecretAccessKey,
		Bucket:          testInsightsBucket,
	}
	cfg, err := parseInsightsStagingS3Config(secret, testInsightsRegion)
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
		SecretAccessKey: testInsightsSecretAccessKey,
	}
	_, err := parseInsightsStagingS3Config(secret, testInsightsRegion)
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestParseInsightsStagingS3Config_MissingRegion(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: testInsightsSecretAccessKey,
		Bucket:          testInsightsBucket,
	}
	_, err := parseInsightsStagingS3Config(secret, "")
	if err == nil {
		t.Fatal("expected error for missing region")
	}
}

func TestParseInsightsStagingS3Config_MissingAccessKeyID(t *testing.T) {
	secret := insightsAwsSecret{
		SecretAccessKey: testInsightsSecretAccessKey,
		Bucket:          testInsightsBucket,
	}
	_, err := parseInsightsStagingS3Config(secret, testInsightsRegion)
	if err == nil {
		t.Fatal("expected error for missing AccessKeyID")
	}
}

func TestParseInsightsStagingS3Config_MissingSecretAccessKey(t *testing.T) {
	secret := insightsAwsSecret{
		AccessKeyID: "AKIATEST",
		Bucket:      testInsightsBucket,
	}
	_, err := parseInsightsStagingS3Config(secret, testInsightsRegion)
	if err == nil {
		t.Fatal("expected error for missing SecretAccessKey")
	}
}
