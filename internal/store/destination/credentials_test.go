package destination

import "testing"

func TestApplyS3CredentialOverrides_overlaysSecretFields(t *testing.T) {
	cfg := &S3Config{
		AuthType: "role_based",
		RoleArn:  "********",
		Region:   "us-east-1",
		Bucket:   testAthenaOutBucket,
	}
	ApplyS3CredentialOverrides(cfg, map[string]string{
		"role_arn":          "arn:aws:iam::999:role/lake",
		"access_key_id":     "AKIA",
		"secret_access_key": "secret",
		"external_id":       "ext-1",
	})
	if cfg.RoleArn != "arn:aws:iam::999:role/lake" {
		t.Fatalf("role_arn = %q", cfg.RoleArn)
	}
	if cfg.AccessKeyID != "AKIA" {
		t.Fatalf("access_key_id = %q", cfg.AccessKeyID)
	}
}

func TestS3ConfigFromExternalConnector_prefersOutputBucket(t *testing.T) {
	cfg := S3ConfigFromExternalConnector(map[string]string{
		"auth_type":     "role_based",
		"role_arn":      "arn:aws:iam::123:role/test",
		"region":        "us-east-1",
		"bucket":        "lake-bucket",
		"output_bucket": testAthenaOutBucket,
	})
	if cfg.Bucket != testAthenaOutBucket {
		t.Fatalf("bucket = %q, want sl-athena-out", cfg.Bucket)
	}
	if cfg.AthenaOutputLocation() != "s3://sl-athena-out/.databahn_out" {
		t.Fatalf("athena output = %q", cfg.AthenaOutputLocation())
	}
}

// A partial or malformed secret must not erase working inline credentials: assigning a blank
// override would fail the export at authentication instead.
func TestApplyS3CredentialOverridesIgnoresBlanks(t *testing.T) {
	cfg := &S3Config{
		AccessKeyID:     "inline-key",
		SecretAccessKey: "inline-secret",
		RoleArn:         "arn:aws:iam::1234:role/inline",
		ExternalID:      "inline-external",
	}
	ApplyS3CredentialOverrides(cfg, map[string]string{
		"access_key_id":     "",
		"secret_access_key": "",
		"role_arn":          "",
		"external_id":       "",
	})
	if cfg.AccessKeyID != "inline-key" || cfg.SecretAccessKey != "inline-secret" {
		t.Fatalf("blank overrides erased key credentials: %+v", cfg)
	}
	if cfg.RoleArn != "arn:aws:iam::1234:role/inline" || cfg.ExternalID != "inline-external" {
		t.Fatalf("blank overrides erased role credentials: %+v", cfg)
	}

	// Non-empty overrides still win.
	ApplyS3CredentialOverrides(cfg, map[string]string{"access_key_id": "from-secret"})
	if cfg.AccessKeyID != "from-secret" {
		t.Fatalf("accessKeyID = %q, want the override applied", cfg.AccessKeyID)
	}
}
