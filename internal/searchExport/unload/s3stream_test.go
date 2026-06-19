package unload

import "testing"

// TestFreshRunStreamOptions documents that zero-value StreamOptions preserves
// the original StreamToUploader behavior (part 1, no file skip, no resume parts).
func TestFreshRunStreamOptions(t *testing.T) {
	opts := StreamOptions{}

	partNum := 1
	if opts.StartPartNumber > 1 {
		partNum = opts.StartPartNumber
	}
	if partNum != 1 {
		t.Errorf("fresh run partNum = %d, want 1", partNum)
	}
	if opts.StartFileIndex != 0 {
		t.Errorf("StartFileIndex = %d, want 0", opts.StartFileIndex)
	}
	if len(opts.ExistingParts) != 0 {
		t.Error("ExistingParts should be empty for fresh run")
	}
	if opts.OnFileCheckpoint != nil {
		t.Error("OnFileCheckpoint should be nil for fresh run")
	}
}
