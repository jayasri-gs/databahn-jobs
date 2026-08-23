package destination

import "testing"

func TestADXConfigFromExternalConnector(t *testing.T) {
	cfg := ADXConfigFromExternalConnector(map[string]string{
		"adx_cluster_uri":     " https://cluster.eastus.kusto.windows.net ",
		"adx_database":        " SecurityLogs ",
		"azure_tenant_id":     "tenant",
		"azure_client_id":     "client",
		"azure_client_secret": "",
	})
	if cfg == nil {
		t.Fatal("expected config")
	}
	if cfg.ClusterURI != "https://cluster.eastus.kusto.windows.net" {
		t.Fatalf("cluster uri = %q", cfg.ClusterURI)
	}
	if cfg.Database != "SecurityLogs" {
		t.Fatalf("database = %q", cfg.Database)
	}
	if ADXConfigFromExternalConnector(nil) != nil {
		t.Fatal("empty connector should produce no config")
	}
}

func TestApplyADXCredentialOverrides(t *testing.T) {
	cfg := &ADXConfig{ClusterURI: "https://c", Database: "db", TenantID: "t", ClientID: "c"}
	ApplyADXCredentialOverrides(cfg, map[string]string{
		"azure_client_secret": "shhh",
		"azure_client_id":     "overridden",
		"unrelated_key":       "ignored",
	})
	if cfg.ClientSecret != "shhh" {
		t.Fatal("client secret was not taken from the override")
	}
	if cfg.ClientID != "overridden" {
		t.Fatalf("client id = %q", cfg.ClientID)
	}
	ApplyADXCredentialOverrides(nil, map[string]string{"azure_client_secret": "x"}) // must not panic
}

func TestADXConfigValidate(t *testing.T) {
	complete := ADXConfig{ClusterURI: "https://cluster.eastus.kusto.windows.net", Database: "db", TenantID: "t", ClientID: "c", ClientSecret: "s"}
	if err := complete.Validate(); err != nil {
		t.Fatalf("complete config rejected: %v", err)
	}

	missing := map[string]ADXConfig{
		"adx_cluster_uri":     {Database: "db", TenantID: "t", ClientID: "c", ClientSecret: "s"},
		"adx_database":        {ClusterURI: "https://c", TenantID: "t", ClientID: "c", ClientSecret: "s"},
		"azure_tenant_id":     {ClusterURI: "https://c", Database: "db", ClientID: "c", ClientSecret: "s"},
		"azure_client_id":     {ClusterURI: "https://c", Database: "db", TenantID: "t", ClientSecret: "s"},
		"azure_client_secret": {ClusterURI: "https://c", Database: "db", TenantID: "t", ClientID: "c"},
	}
	for field, cfg := range missing {
		err := cfg.Validate()
		if err == nil {
			t.Fatalf("missing %s should fail validation", field)
		}
	}

	var nilCfg *ADXConfig
	if err := nilCfg.Validate(); err == nil {
		t.Fatal("nil config should fail validation")
	}
}

// The cluster URI becomes the base URL for requests carrying an Entra bearer token, so it is
// constrained to real Azure Data Explorer hosts rather than anywhere a data store names.
func TestValidateADXClusterURI(t *testing.T) {
	valid := []string{
		"https://cluster.eastus.kusto.windows.net",
		"https://cluster.eastus.kusto.windows.net/",
		"https://help.kusto.windows.net",
		"https://ws.kusto.azuresynapse.net",
		"https://c.kusto.chinacloudapi.cn",
		"https://c.kusto.usgovcloudapi.net",
	}
	for _, uri := range valid {
		if err := ValidateADXClusterURI(uri); err != nil {
			t.Fatalf("ValidateADXClusterURI(%q) = %v, want nil", uri, err)
		}
	}

	invalid := []struct{ name, uri string }{
		{"plain http", "http://cluster.eastus.kusto.windows.net"},
		{"attacker host", "https://attacker.example.com"},
		{"kusto as a subdomain of an attacker host", "https://kusto.windows.net.attacker.example.com"},
		{"bare suffix with no cluster", "https://kusto.windows.net"},
		{"loopback", "https://127.0.0.1"},
		{"loopback name", "https://localhost"},
		{"link-local metadata", "https://169.254.169.254"},
		{"private range", "https://10.0.0.5"},
		{"ipv6 loopback", "https://[::1]"},
		{"embedded credentials", "https://user:pass@cluster.eastus.kusto.windows.net"},
		{"no host", "https://"},
		{"empty", ""},
		{"not a url", "://"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateADXClusterURI(tc.uri); err == nil {
				t.Fatalf("ValidateADXClusterURI(%q) = nil, want an error", tc.uri)
			}
		})
	}
}
