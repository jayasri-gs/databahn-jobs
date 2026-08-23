package query

import "testing"

func TestBuildUnloadSQLRejectsUnsafeOutputPath(t *testing.T) {
	unsafe := []string{
		"s3://bucket/out' WITH (format='CSV') -- ",
		"s3://bucket/out\"put",
		"s3://bucket/out\nput",
		"https://attacker.example.com/collect",
		"s3://bucket/out\\put",
	}
	for _, path := range unsafe {
		if _, err := buildUnloadSQL("SELECT 1", path, UnloadOptions{Format: "json"}); err == nil {
			t.Fatalf("path %q was accepted", path)
		}
	}
}

func TestBuildUnloadSQLQuotesCanonicalS3Path(t *testing.T) {
	got, err := buildUnloadSQL("SELECT 1", "s3://authorized-staging-bucket/.databahn_out/unload_1/", UnloadOptions{Format: "json"})
	if err != nil {
		t.Fatalf("buildUnloadSQL: %v", err)
	}
	want := "UNLOAD (SELECT 1) TO 's3://authorized-staging-bucket/.databahn_out/unload_1/' WITH (format = 'JSON')"
	if got != want {
		t.Fatalf("sql = %q, want %q", got, want)
	}
}
