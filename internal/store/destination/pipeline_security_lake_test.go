package destination

import "testing"

func TestPipelineSecurityLakeConnector_mapsDestinationKeys(t *testing.T) {
	connector, err := pipelineSecurityLakeConnector(map[string]string{
		pipelineAWSRegion:            "us-east-1",
		pipelineSecurityLakeRoleARN:  "arn:aws:iam::123456789012:role/sl-role",
		pipelineAWSExternalID:        "ext-123",
		pipelineCustomSourceLocation: "s3://custom-lake-bucket/custom-source/",
		pipelineAWSAccountID:         "123456789012",
		pipelineOCSFID:               "4002",
		connectorOutputBucket:        "sl-athena-out",
	})
	if err != nil {
		t.Fatalf("pipelineSecurityLakeConnector: %v", err)
	}
	if connector[connectorRegion] != "us-east-1" {
		t.Fatalf("region = %q", connector[connectorRegion])
	}
	if connector[connectorRoleARN] != "arn:aws:iam::123456789012:role/sl-role" {
		t.Fatalf("role_arn = %q", connector[connectorRoleARN])
	}
	if connector[connectorBucket] != "custom-lake-bucket" {
		t.Fatalf("bucket = %q", connector[connectorBucket])
	}
	if connector[connectorOutputBucket] != "sl-athena-out" {
		t.Fatalf("output_bucket = %q", connector[connectorOutputBucket])
	}

	staging := S3ConfigFromExternalConnector(connector)
	if staging.Bucket != "sl-athena-out" {
		t.Fatalf("staging bucket = %q", staging.Bucket)
	}
	if staging.AthenaOutputLocation() != "s3://sl-athena-out/.databahn_out" {
		t.Fatalf("athena output = %q", staging.AthenaOutputLocation())
	}
}

func TestResolvePipelineSecurityLakeBucket_defaultsFromRegion(t *testing.T) {
	got := resolvePipelineSecurityLakeBucket(map[string]string{}, "eu-west-1", "not-a-uri")
	if got != "aws-security-data-lake-eu-west-1" {
		t.Fatalf("bucket = %q", got)
	}
}

func TestResolvePipelineSecurityLakeBucket_rejectsQuotedCustomSource(t *testing.T) {
	got := resolvePipelineSecurityLakeBucket(map[string]string{}, "us-east-1", "s3://evil'/custom-source/")
	if got != "aws-security-data-lake-us-east-1" {
		t.Fatalf("bucket = %q, want the region default", got)
	}
}
