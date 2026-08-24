package destination

import "testing"

func TestSentinelConfigFromExternalConnector(t *testing.T) {
	cfg := SentinelConfigFromExternalConnector(map[string]string{
		"workspace_id":        " ws-guid ",
		"workspace_name":      "prod-sentinel",
		"azure_tenant_id":     "tenant",
		"azure_client_id":     "client",
		"azure_client_secret": "secret",
	})
	if cfg == nil {
		t.Fatal("expected config")
	}
	if cfg.WorkspaceID != "ws-guid" {
		t.Fatalf("workspaceID = %q", cfg.WorkspaceID)
	}
	if cfg.WorkspaceName != "prod-sentinel" {
		t.Fatalf("workspaceName = %q", cfg.WorkspaceName)
	}
	// A blank storage_tier means the analytics tier, matching backend-service.
	if cfg.StorageTier != SentinelTierAnalytics {
		t.Fatalf("storageTier = %q, want ANALYTICS", cfg.StorageTier)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestSentinelConfigFromExternalConnectorEmpty(t *testing.T) {
	if cfg := SentinelConfigFromExternalConnector(nil); cfg != nil {
		t.Fatalf("expected nil for empty connector, got %+v", cfg)
	}
}

func TestNormalizeSentinelTier(t *testing.T) {
	cases := map[string]string{
		"":          SentinelTierAnalytics,
		"  ":        SentinelTierAnalytics,
		"analytics": SentinelTierAnalytics,
		"Lake":      SentinelTierLake,
	}
	for in, want := range cases {
		if got := NormalizeSentinelTier(in); got != want {
			t.Fatalf("NormalizeSentinelTier(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestApplySentinelCredentialOverrides(t *testing.T) {
	cfg := &SentinelConfig{WorkspaceID: "ws"}
	ApplySentinelCredentialOverrides(cfg, map[string]string{
		"azure_tenant_id":     " tenant ",
		"azure_client_id":     "client",
		"azure_client_secret": "from-secrets-manager",
		"unrelated":           "ignored",
	})
	if cfg.TenantID != "tenant" || cfg.ClientID != "client" {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.ClientSecret != "from-secrets-manager" {
		t.Fatal("clientSecret was not taken from the override")
	}
	ApplySentinelCredentialOverrides(nil, map[string]string{"azure_client_id": "x"})
}

func TestSentinelConfigValidate(t *testing.T) {
	base := func() *SentinelConfig {
		return &SentinelConfig{
			WorkspaceID:  "ws",
			StorageTier:  SentinelTierAnalytics,
			TenantID:     "tenant",
			ClientID:     "client",
			ClientSecret: "secret",
		}
	}

	tests := []struct {
		name    string
		mutate  func(*SentinelConfig)
		wantErr string
	}{
		{"valid", func(*SentinelConfig) {
			// Nothing to change: this case asserts the complete config validates.
		}, ""},
		{"missing workspace", func(c *SentinelConfig) { c.WorkspaceID = "" }, "workspace_id is required in connector configuration"},
		{"missing tenant", func(c *SentinelConfig) { c.TenantID = "" }, "azure_tenant_id is required"},
		{"missing client", func(c *SentinelConfig) { c.ClientID = "" }, "azure_client_id is required"},
		{"missing secret", func(c *SentinelConfig) { c.ClientSecret = "" }, "azure_client_secret is required"},
		{
			"lake tier without workspace_name",
			func(c *SentinelConfig) { c.StorageTier = SentinelTierLake },
			"workspace_name is required in connector configuration when storage_tier is LAKE",
		},
		{
			"lake tier with workspace_name",
			func(c *SentinelConfig) {
				c.StorageTier = SentinelTierLake
				c.WorkspaceName = "prod-sentinel"
			},
			"",
		},
		{"unknown tier", func(c *SentinelConfig) { c.StorageTier = "GLACIER" }, "unsupported Sentinel storage_tier: GLACIER"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(cfg)
			err := cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("Validate error = %v, want %q", err, tc.wantErr)
			}
		})
	}

	var nilCfg *SentinelConfig
	if err := nilCfg.Validate(); err == nil {
		t.Fatal("nil config should not validate")
	}
}

// The lake KQL API addresses the workspace by name and GUID joined, not the GUID alone.
func TestSentinelConfigLakeDatabase(t *testing.T) {
	cfg := &SentinelConfig{WorkspaceID: "ws-guid", WorkspaceName: "prod-sentinel"}
	if got := cfg.LakeDatabase(); got != "prod-sentinel-ws-guid" {
		t.Fatalf("LakeDatabase = %q", got)
	}
	for _, incomplete := range []*SentinelConfig{
		nil,
		{WorkspaceID: "ws-guid"},
		{WorkspaceName: "prod-sentinel"},
	} {
		if got := incomplete.LakeDatabase(); got != "" {
			t.Fatalf("LakeDatabase = %q, want empty for %+v", got, incomplete)
		}
	}
}
