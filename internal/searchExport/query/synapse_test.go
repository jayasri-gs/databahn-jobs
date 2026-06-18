package query

import (
	"context"
	"strings"
	"testing"
)

func TestValidateExportQuery_NotConnected(t *testing.T) {
	exec := &SynapseExecutor{}
	if err := exec.ValidateExportQuery(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("expected error")
	}
}

func TestStreamRows_NotConnected(t *testing.T) {
	exec := &SynapseExecutor{}
	_, err := exec.StreamRows(context.Background(), "SELECT 1", StreamRowsOptionsFromEnv(), func([]interface{}) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("err=%v", err)
	}
}
