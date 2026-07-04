package util

import (
	"testing"
	"time"
)

func TestParseDurationWithDays(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{name: "seven days", input: "7d", want: 7 * 24 * time.Hour},
		{name: "one day twelve hours", input: "1d12h", want: 36 * time.Hour},
		{name: "hours only", input: "168h", want: 168 * time.Hour},
		{name: "minutes only", input: "30m", want: 30 * time.Minute},
		{name: "empty", input: "", wantErr: true},
		{name: "invalid", input: "bad", wantErr: true},
		{name: "zero days", input: "0d", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDurationWithDays(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got duration %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}
