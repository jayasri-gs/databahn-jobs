package destination

import "testing"

func TestApplyS3CredentialOverrides_overlaysSecretFields(t *testing.T) {
	cfg := &S3Config{
		AuthType: "role_based",
		RoleArn:  "********",
		Region:   "us-east-1",
		Bucket:   "sl-athena-out",
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
		"output_bucket": "sl-athena-out",
	})
	if cfg.Bucket != "sl-athena-out" {
		t.Fatalf("bucket = %q, want sl-athena-out", cfg.Bucket)
	}
	if cfg.AthenaOutputLocation() != "s3://sl-athena-out/.databahn_out" {
		t.Fatalf("athena output = %q", cfg.AthenaOutputLocation())
	}
}
