package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/state"
)

func TestShouldPreserveQueryCheckpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "athena timeout message", err: fmt.Errorf("query timed out after 30m (execution abc cancelled)"), want: true},
		{name: "context canceled", err: context.Canceled, want: true},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "wrapped deadline", err: fmt.Errorf("wait: %w", context.DeadlineExceeded), want: true},
		{name: "query failed", err: fmt.Errorf("query failed: access denied"), want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldPreserveQueryCheckpoint(tc.err); got != tc.want {
				t.Errorf("shouldPreserveQueryCheckpoint(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestUnloadResultFromCheckpoint(t *testing.T) {
	cp := &state.Checkpoint{
		ManifestLocation: "s3://bucket/manifest",
		TempOutputPath:   "s3://bucket/unload_prefix/",
	}
	got := unloadResultFromCheckpoint(cp)
	if got.ManifestLocation != cp.ManifestLocation {
		t.Errorf("ManifestLocation: got %q want %q", got.ManifestLocation, cp.ManifestLocation)
	}
	if got.OutputLocation != cp.TempOutputPath {
		t.Errorf("OutputLocation: got %q want %q", got.OutputLocation, cp.TempOutputPath)
	}
}
