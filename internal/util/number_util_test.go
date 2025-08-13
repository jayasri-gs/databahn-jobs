package util

import "testing"

func TestHumanReadableNumber(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{500, "500"},
		{1000, "1.00 K"},
		{1500, "1.50 K"},
		{1_500_000, "1.50 M"},
		{1_500_000_000, "1.50 B"},
	}

	for _, tt := range tests {
		got := HumanReadableNumber(tt.input)
		if got != tt.expected {
			t.Errorf("HumanReadableNumber(%d) = %s; want %s", tt.input, got, tt.expected)
		}
	}
}

func TestHumanReadableBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{500, "500 B"},
		{1023, "1023 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1048576, "1.00 MB"},
		{1572864, "1.50 MB"},
	}

	for _, tt := range tests {
		got := HumanReadableBytes(tt.input)
		if got != tt.expected {
			t.Errorf("HumanReadableBytes(%d) = %s; want %s", tt.input, got, tt.expected)
		}
	}
}
