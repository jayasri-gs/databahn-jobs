package destination

import "testing"

func sentinelDestinationConfig() map[string]string {
	return map[string]string{
		"workspace_id":                            " 11111111-2222-3333-4444-555555555555 ",
		"workspace_name":                          "soc-workspace",
		"azure_sentinel_auth_azure_tenant_id":     "tenant-id",
		"azure_sentinel_auth_azure_client_id":     "client-id",
		"azure_sentinel_auth_azure_client_secret": "client-secret",
		"azure_sentinel_stream_name":              "Custom-DatabahnEvents_CL",
	}
}

func TestSentinelTierForDestinationType(t *testing.T) {
	tests := map[string]string{
		DestTypeAzureSentinel:         SentinelTierAnalytics,
		DestTypeAzureSentinelDataLake: SentinelTierLake,
		" azure_sentinel ":            SentinelTierAnalytics,
		"AZURE_BLOB":                  "",
		"":                            "",
	}
	for destType, want := range tests {
		if got := SentinelTierForDestinationType(destType); got != want {
			t.Fatalf("SentinelTierForDestinationType(%q) = %q, want %q", destType, got, want)
		}
	}
}

func TestPipelineSentinelConfig(t *testing.T) {
	cfg, err := pipelineSentinelConfig(sentinelDestinationConfig(), SentinelTierAnalytics)
	if err != nil {
		t.Fatalf("pipelineSentinelConfig: %v", err)
	}
	if cfg.WorkspaceID != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("workspace id = %q", cfg.WorkspaceID)
	}
	if cfg.WorkspaceName != "soc-workspace" || cfg.TenantID != "tenant-id" ||
		cfg.ClientID != "client-id" || cfg.ClientSecret != "client-secret" {
		t.Fatalf("config = %+v", *cfg)
	}
	if cfg.StorageTier != SentinelTierAnalytics {
		t.Fatalf("storage tier = %q", cfg.StorageTier)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("mapped config failed validation: %v", err)
	}
}

// The lake tier addresses the workspace as workspaceName-workspaceId, so a lake destination
// that maps cleanly must also produce a usable db identifier.
func TestPipelineSentinelConfigLakeTier(t *testing.T) {
	cfg, err := pipelineSentinelConfig(sentinelDestinationConfig(), SentinelTierLake)
	if err != nil {
		t.Fatalf("pipelineSentinelConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("lake config failed validation: %v", err)
	}
	if cfg.LakeDatabase() != "soc-workspace-11111111-2222-3333-4444-555555555555" {
		t.Fatalf("lake database = %q", cfg.LakeDatabase())
	}
}

func TestPipelineSentinelConfigRequiresEveryField(t *testing.T) {
	for _, key := range []string{
		"workspace_id",
		"workspace_name",
		"azure_sentinel_auth_azure_tenant_id",
		"azure_sentinel_auth_azure_client_id",
		"azure_sentinel_auth_azure_client_secret",
	} {
		config := sentinelDestinationConfig()
		delete(config, key)
		_, err := pipelineSentinelConfig(config, SentinelTierAnalytics)
		if err == nil {
			t.Fatalf("missing %s should be rejected", key)
		}
		if !contains(err.Error(), key) {
			t.Fatalf("error for missing %s should name the key, got: %v", key, err)
		}
	}

	if _, err := pipelineSentinelConfig(nil, SentinelTierAnalytics); err == nil {
		t.Fatal("empty destination configuration should be rejected")
	}
}
