package destination

import (
	"testing"
)

func TestS3Config_AthenaOutputLocation(t *testing.T) {
	cfg := &S3Config{Bucket: "my-bucket"}
	if got := cfg.AthenaOutputLocation(); got != "s3://my-bucket/.databahn_out" {
		t.Fatalf("got %q", got)
	}
}

func TestParseS3ConfigFromWrapper_InlineCredentials(t *testing.T) {
	wrapper := configWrapper{
		Configuration: map[string]interface{}{
			"auth_type":         "key_based",
			"access_key_id":     "AKIA123",
			"secret_access_key": "secret",
			"region":            "us-east-1",
			"bucket":            "test-bucket",
		},
	}

	cfg, err := parseS3ConfigFromWrapper(wrapper, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AuthType != "key_based" {
		t.Fatalf("auth_type: got %q", cfg.AuthType)
	}
	if cfg.AccessKeyID != "AKIA123" {
		t.Fatalf("access_key_id: got %q", cfg.AccessKeyID)
	}
	if cfg.SecretAccessKey != "secret" {
		t.Fatalf("secret_access_key: got %q", cfg.SecretAccessKey)
	}
	if cfg.Region != "us-east-1" {
		t.Fatalf("region: got %q", cfg.Region)
	}
	if cfg.Bucket != "test-bucket" {
		t.Fatalf("bucket: got %q", cfg.Bucket)
	}
}

func TestParseS3ConfigFromWrapper_RoleBased(t *testing.T) {
	wrapper := configWrapper{
		Configuration: map[string]interface{}{
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
	if cfg.AuthType != "role_based" {
		t.Fatalf("auth_type: got %q", cfg.AuthType)
	}
	if cfg.RoleArn != "arn:aws:iam::123:role/test" {
		t.Fatalf("role_arn: got %q", cfg.RoleArn)
	}
	if cfg.ExternalID != "ext-123" {
		t.Fatalf("external_id: got %q", cfg.ExternalID)
	}
}

func TestParseS3ConfigFromWrapper_SecretOverlay(t *testing.T) {
	wrapper := configWrapper{
		Configuration: map[string]interface{}{
			"access_key_id":     "inline-key",
			"secret_access_key": "inline-secret",
			"region":            "eu-west-1",
			"bucket":            "overlay-bucket",
		},
	}
	secret := map[string]string{
		"access_key_id":     "secret-key",
		"secret_access_key": "secret-val",
		"role_arn":          "arn:aws:iam::456:role/from-secret",
	}

	cfg, err := parseS3ConfigFromWrapper(wrapper, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AccessKeyID != "secret-key" {
		t.Fatalf("access_key_id: got %q", cfg.AccessKeyID)
	}
	if cfg.SecretAccessKey != "secret-val" {
		t.Fatalf("secret_access_key: got %q", cfg.SecretAccessKey)
	}
	if cfg.RoleArn != "arn:aws:iam::456:role/from-secret" {
		t.Fatalf("role_arn: got %q", cfg.RoleArn)
	}
}

func TestParseS3ConfigFromWrapper_MissingBucket(t *testing.T) {
	wrapper := configWrapper{
		Configuration: map[string]interface{}{
			"region": "us-east-1",
		},
	}
	_, err := parseS3ConfigFromWrapper(wrapper, nil)
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestParseS3ConfigFromWrapper_MissingRegion(t *testing.T) {
	wrapper := configWrapper{
		Configuration: map[string]interface{}{
			"bucket": "only-bucket",
		},
	}
	_, err := parseS3ConfigFromWrapper(wrapper, nil)
	if err == nil {
		t.Fatal("expected error for missing region")
	}
}
