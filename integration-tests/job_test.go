//go:build integration

package integrationtests

import (
	"context"
	"testing"

	"github.com/databahn-ai/pramaan-go/pramaan"
)

func TestUnknownJobFails(t *testing.T) {
	tg := pramaan.NewGoTestLogger(t)

	result := JobPramaan().RunUnchecked(context.Background(), tg, pramaan.JobRunOptions{
		Cmd: []string{"-job", "not-a-real-job"},
	})

	if result.ExitCode == 0 {
		t.Fatalf("expected non-zero exit code for unknown job, got %d", result.ExitCode)
	}
}
