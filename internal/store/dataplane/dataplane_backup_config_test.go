package dataplane

import (
	"encoding/json"
	"testing"
)

func TestParseBackupConfiguration_DatabahnStorageRegion(t *testing.T) {
	dp := &DataPlane{
		BackupConfiguration: json.RawMessage(`{
			"unparsedConfiguration": {
				"awsConfiguration": {"bucket": "unparsed-bucket", "region": "us-east-1"}
			},
			"sandboxConfiguration": {
				"awsConfiguration": {"bucket": "sandbox-bucket", "region": "eu-west-1"}
			},
			"databahnStorageConfiguration": {"region": "ap-south-1"}
		}`),
	}

	cfg, err := dp.ParseBackupConfiguration()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected backup configuration")
	}
	if cfg.DatabahnStorageConfiguration.Region != "ap-south-1" {
		t.Fatalf("databahn storage region: got %q, want %q", cfg.DatabahnStorageConfiguration.Region, "ap-south-1")
	}
	if cfg.UnparsedConfiguration.AWSConfiguration.Region != "us-east-1" {
		t.Fatalf("unparsed region changed: got %q", cfg.UnparsedConfiguration.AWSConfiguration.Region)
	}
	if cfg.SandboxConfiguration.AWSConfiguration.Region != "eu-west-1" {
		t.Fatalf("sandbox region changed: got %q", cfg.SandboxConfiguration.AWSConfiguration.Region)
	}
}

func TestParseBackupConfiguration_MissingDatabahnStorageConfiguration(t *testing.T) {
	dp := &DataPlane{
		BackupConfiguration: json.RawMessage(`{
			"unparsedConfiguration": {
				"awsConfiguration": {"bucket": "unparsed-bucket", "region": "us-east-1"}
			}
		}`),
	}

	cfg, err := dp.ParseBackupConfiguration()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DatabahnStorageConfiguration.Region != "" {
		t.Fatalf("expected empty databahn storage region, got %q", cfg.DatabahnStorageConfiguration.Region)
	}
}
