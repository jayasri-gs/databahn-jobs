package util

import "testing"

// Contains checks if a string is present in a slice of strings
func TestContains(t *testing.T) {
	l := []string{"a", "b", "c"}
	key := "b"
	if !Contains(l, key) {
		t.Errorf("Expected true, got false")
	}
	key = "d"
	if Contains(l, key) {
		t.Errorf("Expected false, got true")
	}
}
