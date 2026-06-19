package query

import (
	"testing"
)

func TestCheckCellSizeWithinLOBLimit(t *testing.T) {
	if err := checkCellSize("ok"); err != nil {
		t.Fatal(err)
	}
	big := make([]byte, maxLOBBytes+1)
	if err := checkCellSize(big); err == nil {
		t.Fatal("expected LOB error")
	}
}

func TestNormalizeCellValue(t *testing.T) {
	if got := normalizeCellValue([]byte("hi")); got != "hi" {
		t.Fatalf("got %v", got)
	}
	if got := normalizeCellValue(int64(7)); got != int64(7) {
		t.Fatalf("got %v", got)
	}
}
