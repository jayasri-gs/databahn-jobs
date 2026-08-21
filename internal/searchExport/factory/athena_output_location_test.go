package factory

import (
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

const testStagingBucket = "authorized-staging-bucket"

func testStaging() *destination.S3Config {
	return &destination.S3Config{Region: "us-east-1", Bucket: testStagingBucket}
}

func TestAthenaOutputLocationForDerivesFromStaging(t *testing.T) {
	got, err := athenaOutputLocationFor(testStaging())
	if err != nil {
		t.Fatalf("athenaOutputLocationFor: %v", err)
	}
	if want := "s3://" + testStagingBucket + "/.databahn_out"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

// The bucket lands in the UNLOAD statement's single-quoted SQL literal, so a name outside
// AWS's grammar — which cannot contain a quote or whitespace — is refused.
func TestAthenaOutputLocationForRejectsInvalidBucket(t *testing.T) {
	cases := []struct{ name, bucket string }{
		{"empty", ""},
		{"whitespace", "   "},
		{"uppercase", "Authorized-Bucket"},
		{"underscore", "authorized_bucket"},
		{"too short", "ab"},
		{"leading dot", ".bucket"},
		{"trailing dot", "bucket."},
		{"single quote", "bucket'; DROP"},
		{"space", "bucket name"},
		{"slash", "bucket/key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := athenaOutputLocationFor(&destination.S3Config{Bucket: tc.bucket})
			if err == nil {
				t.Fatalf("bucket %q was accepted", tc.bucket)
			}
		})
	}
}

// The security property this file exists to protect: whatever the report configuration says
// the Athena output location should be, the worker ignores it and uses its own staging
// bucket. Nothing from report_configuration reaches the Athena client or the UNLOAD
// statement, so there is no value to sanitize.
func TestResolveAthenaClientConfigIgnoresReportConfiguredOutputLocation(t *testing.T) {
	hostile := []string{
		"s3://attacker-bucket/.databahn_out",
		"s3://" + testStagingBucket + "/out' WITH (format='CSV') -- ",
		"https://attacker.example.com/collect",
		"s3://" + testStagingBucket + "/out\nX-Injected: true",
		"s3://" + testStagingBucket + "/out\x00",
	}
	want := "s3://" + testStagingBucket + "/.databahn_out"

	for _, location := range hostile {
		got, err := resolveAthenaClientConfig(
			&models.SearchExportConfig{AthenaOutputLocation: location}, testStaging())
		if err != nil {
			t.Fatalf("resolveAthenaClientConfig(%q) = %v, want the derived location", location, err)
		}
		if got.OutputLocation != want {
			t.Fatalf("outputLocation = %q, want the derived %q", got.OutputLocation, want)
		}
	}
}

func TestResolveAthenaClientConfigStillHonoursRegionOverride(t *testing.T) {
	got, err := resolveAthenaClientConfig(&models.SearchExportConfig{Region: "eu-west-1"}, testStaging())
	if err != nil {
		t.Fatalf("resolveAthenaClientConfig: %v", err)
	}
	if got.Region != "eu-west-1" {
		t.Fatalf("region = %q, want the override applied", got.Region)
	}
	if want := "s3://" + testStagingBucket + "/.databahn_out"; got.OutputLocation != want {
		t.Fatalf("outputLocation = %q, want %q", got.OutputLocation, want)
	}
}
