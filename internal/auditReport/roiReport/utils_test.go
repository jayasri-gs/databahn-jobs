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
			incoming: "1000.5",
			outgoing: "300.15",
			expected: "70.00",
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
