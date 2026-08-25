package destination

import (
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/store/dataplane"
	"github.com/google/uuid"
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

func TestParseDatabahnStorageStagingConfig_IgnoresGenericRegionKey(t *testing.T) {
	cfgMap := map[string]string{
		"region":       "eu-west-1",
		"s3BucketName": testDatabahnStorageBucket,
	}
	_, err := parseDatabahnStorageStagingConfig(cfgMap)
	if err == nil {
		t.Fatal("expected error when only generic region is set; DATABAHN_STORAGE requires s3Region")
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

func TestResolveDatabahnStorageRegion_DestS3RegionWins(t *testing.T) {
	got, err := resolveDatabahnStorageRegion("ap-south-1", "us-west-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "ap-south-1" {
		t.Fatalf("got %q, want dest s3Region", got)
	}
}

func TestResolveDatabahnStorageRegion_FallbackToDataplane(t *testing.T) {
	got, err := resolveDatabahnStorageRegion("", "us-west-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "us-west-2" {
		t.Fatalf("got %q, want dataplane region", got)
	}
}

func TestResolveDatabahnStorageRegion_WhitespaceDestUsesDataplane(t *testing.T) {
	got, err := resolveDatabahnStorageRegion("  ", "eu-central-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "eu-central-1" {
		t.Fatalf("got %q, want dataplane region", got)
	}
}

func TestResolveDatabahnStorageRegion_MissingBothErrors(t *testing.T) {
	_, err := resolveDatabahnStorageRegion("", "")
	if err == nil {
		t.Fatal("expected error when dest and dataplane region are missing")
	}
	want := "Databahn Storage is not enabled for this data plane. Please contact your administrator."
	if err.Error() != want {
		t.Fatalf("got %q, want %q", err.Error(), want)
	}
}

func TestResolveDatabahnStorageRegion_DoesNotReceiveGenericRegionKey(t *testing.T) {
	// Callers pass cfgMap["s3Region"] only. Generic "region" must not be treated as dest region.
	got, err := resolveDatabahnStorageRegion("", "ap-south-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "ap-south-1" {
		t.Fatalf("got %q, want dataplane fallback", got)
	}
}

func TestDatabahnStorageRegionFromDataPlane_Present(t *testing.T) {
	dp := &dataplane.DataPlane{
		BackupConfiguration: []byte(`{"databahnStorageConfiguration":{"region":"us-west-2"}}`),
	}
	got, err := databahnStorageRegionFromDataPlane(dp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "us-west-2" {
		t.Fatalf("got %q, want us-west-2", got)
	}
}

func TestDatabahnStorageRegionFromDataPlane_EmptyBackup(t *testing.T) {
	dp := &dataplane.DataPlane{}
	got, err := databahnStorageRegionFromDataPlane(dp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestDatabahnStorageRegionFromDataPlane_MalformedJSON(t *testing.T) {
	dp := &dataplane.DataPlane{BackupConfiguration: []byte(`{`)}
	_, err := databahnStorageRegionFromDataPlane(dp)
	if err == nil {
		t.Fatal("expected error for malformed backup JSON")
	}
}

func TestDatabahnStorageRegionFromJoinRow_NoDataPlaneID(t *testing.T) {
	_, err := databahnStorageRegionFromJoinRow(nil, nil, nil)
	if err == nil {
		t.Fatal("expected error when destination has no data plane")
	}
	if err.Error() != "destination has no data plane" {
		t.Fatalf("got %q", err.Error())
	}
}

func TestDatabahnStorageRegionFromJoinRow_NilDataPlaneUUID(t *testing.T) {
	id := uuid.Nil
	_, err := databahnStorageRegionFromJoinRow(&id, nil, nil)
	if err == nil || err.Error() != "destination has no data plane" {
		t.Fatalf("got %v", err)
	}
}

func TestDatabahnStorageRegionFromJoinRow_DataPlaneMissing(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	_, err := databahnStorageRegionFromJoinRow(&id, nil, nil)
	if err == nil {
		t.Fatal("expected error when dataplane row is missing")
	}
	want := "data plane not found with id: " + id.String()
	if err.Error() != want {
		t.Fatalf("got %q, want %q", err.Error(), want)
	}
}

func TestDatabahnStorageRegionFromJoinRow_Present(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	got, err := databahnStorageRegionFromJoinRow(
		&id,
		&id,
		[]byte(`{"databahnStorageConfiguration":{"region":"us-west-2"}}`),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "us-west-2" {
		t.Fatalf("got %q, want us-west-2", got)
	}
}

func TestLoadDatabahnStorageStagingConfig_FillsS3RegionThenParses(t *testing.T) {
	cfgMap := map[string]string{
		"s3BucketName": testDatabahnStorageBucket,
	}
	region, err := resolveDatabahnStorageRegion(cfgMap["s3Region"], "ap-south-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cfgMap["s3Region"] = region
	cfg, err := parseDatabahnStorageStagingConfig(cfgMap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Region != "ap-south-1" {
		t.Fatalf("got region %q", cfg.Region)
	}
}

func TestLoadDatabahnStorageStagingConfig_GenericRegionStillIgnored(t *testing.T) {
	cfgMap := map[string]string{
		"region":       "eu-west-1",
		"s3BucketName": testDatabahnStorageBucket,
	}
	region, err := resolveDatabahnStorageRegion(cfgMap["s3Region"], "ap-south-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cfgMap["s3Region"] = region
	cfg, err := parseDatabahnStorageStagingConfig(cfgMap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Region != "ap-south-1" {
		t.Fatalf("generic region must be ignored, got %q", cfg.Region)
	}
}
