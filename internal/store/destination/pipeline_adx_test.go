package destination

import "testing"

func adxDestinationConfig() map[string]string {
	return map[string]string{
		"adx_cluster_endpoint_url": " https://cluster.eastus.kusto.windows.net ",
		"adx_database_name":        "SecurityLogs",
		"adx_table_name":           "DatabahnEvents",
		"adx_tenant_id":            "tenant-id",
		"adx_client_id":            "client-id",
		"adx_client_secret":        "client-secret",
	}
}

func TestPipelineADXConfig(t *testing.T) {
	cfg, err := pipelineADXConfig(adxDestinationConfig())
	if err != nil {
		t.Fatalf("pipelineADXConfig: %v", err)
	}
	if cfg.ClusterURI != "https://cluster.eastus.kusto.windows.net" {
		t.Fatalf("cluster uri = %q", cfg.ClusterURI)
	}
	if cfg.Database != "SecurityLogs" || cfg.TenantID != "tenant-id" ||
		cfg.ClientID != "client-id" || cfg.ClientSecret != "client-secret" {
		t.Fatalf("config = %+v", *cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("mapped config failed validation: %v", err)
	}
}

func TestPipelineADXConfigRequiresEveryField(t *testing.T) {
	for _, key := range []string{
		"adx_cluster_endpoint_url",
		"adx_database_name",
		"adx_tenant_id",
		"adx_client_id",
		"adx_client_secret",
	} {
		config := adxDestinationConfig()
		delete(config, key)
		_, err := pipelineADXConfig(config)
		if err == nil {
			t.Fatalf("missing %s should be rejected", key)
		}
		if !contains(err.Error(), key) {
			t.Fatalf("error for missing %s should name the key, got: %v", key, err)
		}
	}

	if _, err := pipelineADXConfig(nil); err == nil {
		t.Fatal("empty destination configuration should be rejected")
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
