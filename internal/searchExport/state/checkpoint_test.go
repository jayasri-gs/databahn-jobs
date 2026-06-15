package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	cp := Checkpoint{
		Stage:             StageQuerying,
		AthenaExecutionID: "exec-123",
	}
	if err := Write(dir, "report-1", cp); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := Read(dir, "report-1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.AthenaExecutionID != "exec-123" {
		t.Errorf("AthenaExecutionID: got %q want %q", got.AthenaExecutionID, "exec-123")
	}
	if got.Stage != StageQuerying {
		t.Errorf("Stage: got %q want %q", got.Stage, StageQuerying)
	}
}

func TestReadMissing(t *testing.T) {
	dir := t.TempDir()
	got, err := Read(dir, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for missing checkpoint, got %+v", got)
	}
}

func TestWriteIsAtomic(t *testing.T) {
	dir := t.TempDir()
	if err := Write(dir, "r1", Checkpoint{Stage: StageQuerying, AthenaExecutionID: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := Write(dir, "r1", Checkpoint{Stage: StageUploading, UploadID: "uid-456"}); err != nil {
		t.Fatal(err)
	}
	got, _ := Read(dir, "r1")
	if got.UploadID != "uid-456" {
		t.Errorf("expected uid-456, got %q", got.UploadID)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "r1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "checkpoint.json" {
			t.Errorf("unexpected file left in checkpoint dir: %s", e.Name())
		}
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	_ = Write(dir, "r1", Checkpoint{Stage: StageQuerying})
	if err := Delete(dir, "r1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := Read(dir, "r1")
	if err != nil {
		t.Fatalf("Read after delete: %v", err)
	}
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestUpdatedAtSetOnWrite(t *testing.T) {
	dir := t.TempDir()
	before := time.Now().UTC()
	_ = Write(dir, "r1", Checkpoint{Stage: StageQuerying})
	got, _ := Read(dir, "r1")
	if got.UpdatedAt.Before(before) {
		t.Errorf("UpdatedAt not refreshed on write: got %v", got.UpdatedAt)
	}
}
