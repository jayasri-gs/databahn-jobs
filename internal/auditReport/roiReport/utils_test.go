package roiReport

import (
	"testing"
)

func TestCalculateReductionPercentage(t *testing.T) {
	tests := []struct {
		name     string
		incoming string
		outgoing string
		expected string
		wantErr  bool
	}{
		{
			name:     "50% reduction",
			incoming: "1000",
			outgoing: "500",
			expected: "50.00",
		},
		{
			name:     "no reduction",
			incoming: "1000",
			outgoing: "1000",
			expected: "0.00",
		},
		{
			name:     "full reduction",
			incoming: "1000",
			outgoing: "0",
			expected: "100.00",
		},
		{
			name:     "outgoing exceeds incoming floors at 0",
			incoming: "500",
			outgoing: "1000",
			expected: "0.00",
		},
		{
			name:     "empty incoming returns 0",
			incoming: "",
			outgoing: "500",
			expected: "0.00",
		},
		{
			name:     "large byte values",
			incoming: "10485760",
			outgoing: "5242880",
			expected: "50.00",
		},
		{
			name:     "fractional values",
			incoming: "1000",
			outgoing: "333.5",
			expected: "66.65",
		},
		{
			name:     "very small reduction",
			incoming: "1000000",
			outgoing: "999999",
			expected: "0.00",
		},
		{
			name:     "zero incoming and zero outgoing",
			incoming: "0",
			outgoing: "0",
			expected: "0.00",
		},
		{
			name:     "invalid incoming value",
			incoming: "abc",
			outgoing: "500",
			wantErr:  true,
		},
		{
			name:     "invalid outgoing value",
			incoming: "1000",
			outgoing: "xyz",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateReductionPercentage(tt.incoming, tt.outgoing)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none, result: %s", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("calculateReductionPercentage(%q, %q) = %q, want %q",
					tt.incoming, tt.outgoing, result, tt.expected)
			}
		})
	}
}

func TestHumanizeBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "zero", input: "0", expected: "0 B"},
		{name: "bytes", input: "512", expected: "512 B"},
		{name: "1 KiB", input: "1024", expected: "1.0 KiB"},
		{name: "fractional KiB", input: "1536", expected: "1.5 KiB"},
		{name: "1 MiB", input: "1048576", expected: "1.0 MiB"},
		{name: "512 MiB", input: "536870912", expected: "512 MiB"},
		{name: "1 GiB", input: "1073741824", expected: "1.0 GiB"},
		{name: "1 TiB", input: "1099511627776", expected: "1.0 TiB"},
		{name: "1 PiB", input: "1125899906842624", expected: "1.0 PiB"},
		{name: "empty string", input: "", expected: "0 B"},
		{name: "invalid string", input: "abc", expected: "0 B"},
		{name: "negative value", input: "-1024", expected: "0 B"},
		{name: "large realistic value", input: "5368709120", expected: "5.0 GiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := humanizeBytes(tt.input)
			if result != tt.expected {
				t.Errorf("humanizeBytes(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
