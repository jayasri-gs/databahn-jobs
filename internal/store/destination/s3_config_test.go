package destination

import (
	"testing"
)

const testDatabahnStorageBucket = "databahn-storage-bucket"

func TestS3Config_AthenaOutputLocation(t *testing.T) {
	cfg := &S3Config{Bucket: "my-bucket"}
	if got := cfg.AthenaOutputLocation(); got != "s3://my-bucket/.databahn_out" {
		t.Fatalf("unexpected output location")
	}
}

func assertField(t *testing.T, field, want, got string, sensitive bool) {
	t.Helper()
	if want == got {
		return
	}
	if sensitive {
		t.Fatalf("%s mismatch", field)
	}
	t.Fatalf("%s: got %q, want %q", field, got, want)
}

func TestParseS3ConfigFromWrapper_InlineCredentials(t *testing.T) {
	wrapper := ConfigWrapper{
		Configuration: map[string]string{
			"auth_type":         "key_based",
			"access_key_id":     "test-access-key-id",
			"secret_access_key": "test-secret-value",
			"region":            "us-east-1",
			"bucket":            "test-bucket",
		},
	}

	cfg, err := parseS3ConfigFromWrapper(wrapper, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertField(t, "auth_type", "key_based", cfg.AuthType, false)
	assertField(t, "access_key_id", "test-access-key-id", cfg.AccessKeyID, true)
	assertField(t, "secret_access_key", "test-secret-value", cfg.SecretAccessKey, true)
	assertField(t, "region", "us-east-1", cfg.Region, false)
	assertField(t, "bucket", "test-bucket", cfg.Bucket, false)
}

func TestParseS3ConfigFromWrapper_RoleBased(t *testing.T) {
	wrapper := ConfigWrapper{
		Configuration: map[string]string{
			"auth_type":   "role_based",
			"role_arn":    "arn:aws:iam::123:role/test",
			"external_id": "ext-123",
			"region":      "us-west-2",
			"bucket":      "role-bucket",
		},
	}

	cfg, err := parseS3ConfigFromWrapper(wrapper, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertField(t, "auth_type", "role_based", cfg.AuthType, false)
	assertField(t, "role_arn", "arn:aws:iam::123:role/test", cfg.RoleArn, false)
	assertField(t, "external_id", "ext-123", cfg.ExternalID, false)
}

func TestParseS3ConfigFromWrapper_SecretOverlay(t *testing.T) {
	wrapper := ConfigWrapper{
		Configuration: map[string]string{
			"access_key_id":     "inline-key",
			"secret_access_key": "inline-value",
			"region":            "eu-west-1",
			"bucket":            "overlay-bucket",
		},
	}
	credentialOverrides := map[string]string{
		"access_key_id":     "override-key",
		"secret_access_key": "override-value",
		"role_arn":          "arn:aws:iam::456:role/from-override",
	}

	cfg, err := parseS3ConfigFromWrapper(wrapper, credentialOverrides)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertField(t, "access_key_id", "override-key", cfg.AccessKeyID, true)
	assertField(t, "secret_access_key", "override-value", cfg.SecretAccessKey, true)
	assertField(t, "role_arn", "arn:aws:iam::456:role/from-override", cfg.RoleArn, false)
}

func TestParseS3ConfigFromWrapper_MissingBucket(t *testing.T) {
	wrapper := ConfigWrapper{
		Configuration: map[string]string{
			"region": "us-east-1",
		},
	}
	_, err := parseS3ConfigFromWrapper(wrapper, nil)
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestParseS3ConfigFromWrapper_MissingRegion(t *testing.T) {
	wrapper := ConfigWrapper{
		Configuration: map[string]string{
			"bucket": "only-bucket",
		},
	}
	_, err := parseS3ConfigFromWrapper(wrapper, nil)
	if err == nil {
		t.Fatal("expected error for missing region")
	}
}

func TestParseDatabahnStorageStagingConfig_Valid(t *testing.T) {
	cfgMap := map[string]string{
		"s3Region":     "ap-south-1",
		"s3BucketName": testDatabahnStorageBucket,
	}
	cfg, err := parseDatabahnStorageStagingConfig(cfgMap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertField(t, "region", "ap-south-1", cfg.Region, false)
	assertField(t, "bucket", testDatabahnStorageBucket, cfg.Bucket, false)
}

func TestParseDatabahnStorageStagingConfig_NoCredentials(t *testing.T) {
	cfgMap := map[string]string{
		"s3Region":     "us-east-1",
		"s3BucketName": testDatabahnStorageBucket,
	}
	cfg, err := parseDatabahnStorageStagingConfig(cfgMap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" || cfg.RoleArn != "" {
		t.Fatal("DATABAHN_STORAGE staging config must not contain stored credentials")
	}
	if cfg.AuthType != "" {
		t.Fatalf("DATABAHN_STORAGE staging config AuthType must be empty (platform default), got %q", cfg.AuthType)
	}
}

func TestParseDatabahnStorageStagingConfig_MissingRegion(t *testing.T) {
	cfgMap := map[string]string{
		"s3BucketName": testDatabahnStorageBucket,
	}
	_, err := parseDatabahnStorageStagingConfig(cfgMap)
	if err == nil {
		t.Fatal("expected error for missing s3Region")
	}
}

func TestParseDatabahnStorageStagingConfig_MissingBucket(t *testing.T) {
	cfgMap := map[string]string{
		"s3Region": "us-east-1",
	}
	_, err := parseDatabahnStorageStagingConfig(cfgMap)
	if err == nil {
		t.Fatal("expected error for missing s3BucketName")
	}
}
