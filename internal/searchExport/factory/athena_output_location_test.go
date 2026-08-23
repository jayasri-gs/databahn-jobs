package factory

import (
	"strings"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

const (
	testStagingBucket = "authorized-staging-bucket"
	testBucket        = "my-bucket"
	// Reuses the production constant so the tests cannot drift from the real key.
	testOutputSuffix = "/" + databahnAthenaOutputPrefix
)

func testStaging() *destination.S3Config {
	return &destination.S3Config{Region: "us-east-1", Bucket: testStagingBucket}
}

func TestAthenaOutputLocationForDerivesFromStaging(t *testing.T) {
	got, err := athenaOutputLocationFor(testStaging())
	if err != nil {
		t.Fatalf("athenaOutputLocationFor: %v", err)
	}
	if want := "s3://" + testStagingBucket + testOutputSuffix; got != want {
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
	want := "s3://" + testStagingBucket + testOutputSuffix

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

func TestValidateAthenaOutputLocation(t *testing.T) {
	valid := []struct{ location, bucket string }{
		{"s3://amazon-security-lake-us-east-1/.databahn_out", "amazon-security-lake-us-east-1"},
		{"s3://" + testBucket + testOutputSuffix, testBucket},
		{"s3://my.bucket.with.dots/prefix/nested", "my.bucket.with.dots"},
		{"s3://abc", "abc"},
		{"s3://my-bucket", testBucket},
		{"s3://my-bucket/", testBucket},
	}
	for _, tc := range valid {
		if err := validateAthenaOutputLocation(tc.location, tc.bucket); err != nil {
			t.Fatalf("validateAthenaOutputLocation(%q, %q) = %v, want nil", tc.location, tc.bucket, err)
		}
	}

	invalid := []struct {
		name     string
		location string
	}{
		{"empty", ""},
		{"wrong scheme", "https://my-bucket/.databahn_out"},
		{"no scheme", "my-bucket/.databahn_out"},
		{"file scheme", "file:///etc/passwd"},
		{"uppercase bucket", "s3://My-Bucket/out"},
		{"underscore in bucket", "s3://my_bucket/out"},
		{"bucket too short", "s3://ab"},
		{"leading dot in bucket", "s3://.bucket/out"},
		{"trailing dot in bucket", "s3://bucket./out"},
		{"embedded space", "s3://my-bucket/out put"},
		{"single quote", "s3://my-bucket/out'ration"},
		{"double quote", `s3://my-bucket/out"put`},
		{"backslash", `s3://my-bucket/out\put`},
		{"newline", "s3://my-bucket/out\nput"},
		{"carriage return", "s3://my-bucket/out\rput"},
		{"null byte", "s3://my-bucket/out\x00put"},
		{"del character", "s3://my-bucket/out\x7fput"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateAthenaOutputLocation(tc.location, testBucket); err == nil {
				t.Fatalf("validateAthenaOutputLocation(%q) = nil, want an error", tc.location)
			}
		})
	}
}

func TestValidateAthenaOutputLocationBindsToStagingBucket(t *testing.T) {
	const staging = "authorized-staging-bucket"

	if err := validateAthenaOutputLocation("s3://"+staging+testOutputSuffix, staging); err != nil {
		t.Fatalf("matching bucket rejected: %v", err)
	}

	err := validateAthenaOutputLocation("s3://attacker-bucket/.databahn_out", staging)
	if err == nil {
		t.Fatal("expected an unauthorized bucket to be rejected")
	}
	if !strings.Contains(err.Error(), "authorized staging bucket") {
		t.Fatalf("error = %v, want it to name the authorized bucket", err)
	}

	if err := validateAthenaOutputLocation("s3://"+staging+"-evil/out", staging); err == nil {
		t.Fatal("expected a prefix-matching bucket to be rejected")
	}

	if err := validateAthenaOutputLocation("s3://any-bucket/out", ""); err == nil {
		t.Fatal("expected an empty authorized bucket to be rejected")
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
	if want := "s3://" + testStagingBucket + testOutputSuffix; got.OutputLocation != want {
		t.Fatalf("outputLocation = %q, want %q", got.OutputLocation, want)
	}
}
