package insights

import (
	"testing"
)

func TestParseDeviceAggModeFromEnv(t *testing.T) {
	t.Setenv(deviceAggEnv, " agg ")
	if got := parseDeviceAggModeFromEnv(); got != "AGG" {
		t.Fatalf("parseDeviceAggModeFromEnv() = %q, want AGG", got)
	}

	t.Setenv(deviceAggEnv, "invalid")
	if got := parseDeviceAggModeFromEnv(); got != "" {
		t.Fatalf("parseDeviceAggModeFromEnv() = %q, want empty for invalid value", got)
	}
}

func TestDeviceAggModeCachedPerJobRun(t *testing.T) {
	t.Setenv(deviceAggEnv, "AGG")
	loadDeviceAggMode()
	if !deviceAggEnabled() {
		t.Fatal("expected device agg enabled after load")
	}

	t.Setenv(deviceAggEnv, "")
	loadDeviceAggMode()
	if deviceAggEnabled() {
		t.Fatal("expected device agg disabled after reload with empty env")
	}
}

func TestParseTestSkipInsightsObjectStoreUploadFromEnv(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "false", value: "false", want: false},
		{name: "true", value: "true", want: true},
		{name: "TRUE", value: "TRUE", want: true},
		{name: "empty string invalid", value: "", want: false},
		{name: "typo invalid", value: "flase", want: false},
		{name: "whitespace true", value: " true ", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(insightsTestSkipObjectStoreUploadEnv, tt.value)
			if got := parseTestSkipInsightsObjectStoreUploadFromEnv(); got != tt.want {
				t.Fatalf("parseTestSkipInsightsObjectStoreUploadFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}
