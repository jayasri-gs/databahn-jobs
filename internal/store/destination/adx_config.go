package destination

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ADX connector configuration keys, mirroring backend-service AzureConstants.
const (
	adxClusterURIKey     = "adx_cluster_uri"
	adxDatabaseKey       = "adx_database"
	azureTenantIDKey     = "azure_tenant_id"
	azureClientIDKey     = "azure_client_id"
	azureClientSecretKey = "azure_client_secret"
)

// adxClusterHostSuffixes are the Azure Data Explorer endpoints an export may target, across
// Azure public, sovereign and Synapse clusters.
var adxClusterHostSuffixes = []string{
	".kusto.windows.net",
	".kusto.chinacloudapi.cn",
	".kusto.usgovcloudapi.net",
	".kusto.azuresynapse.net",
}

// ValidateADXClusterURI constrains where the export worker will send an Entra bearer token.
//
// The cluster URI is tenant-supplied configuration and becomes the base URL for outbound
// requests carrying an access token minted for that same URI's scope. Without a host
// allowlist, a crafted data store could point it at an attacker-controlled endpoint and
// collect the token, so anything that is not an HTTPS Azure Data Explorer host is refused —
// including IP literals, which covers loopback, private and link-local targets.
func ValidateADXClusterURI(raw string) error {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("adx_cluster_uri %q is not a valid URL: %w", trimmed, err)
	}
	if !strings.EqualFold(parsed.Scheme, "https") {
		return fmt.Errorf("adx_cluster_uri must use https, got %q", parsed.Scheme)
	}
	if parsed.User != nil {
		return fmt.Errorf("adx_cluster_uri must not embed credentials")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return fmt.Errorf("adx_cluster_uri %q has no host", trimmed)
	}
	if net.ParseIP(host) != nil {
		return fmt.Errorf("adx_cluster_uri must name an Azure Data Explorer host, not the IP literal %q", host)
	}
	for _, suffix := range adxClusterHostSuffixes {
		if len(host) > len(suffix) && strings.HasSuffix(host, suffix) {
			return nil
		}
	}
	return fmt.Errorf("adx_cluster_uri host %q is not an Azure Data Explorer endpoint", host)
}

// ADXConfig holds the credentials needed to run KQL and .export control commands
// against an Azure Data Explorer cluster.
type ADXConfig struct {
	ClusterURI   string
	Database     string
	TenantID     string
	ClientID     string
	ClientSecret string
}

// ADXConfigFromExternalConnector maps an EXTERNAL_STORAGE connectorConfig onto an ADXConfig.
// The client secret is normally blank here and supplied by ApplyADXCredentialOverrides.
func ADXConfigFromExternalConnector(connector map[string]string) *ADXConfig {
	if len(connector) == 0 {
		return nil
	}
	return &ADXConfig{
		ClusterURI:   strings.TrimSpace(connector[adxClusterURIKey]),
		Database:     strings.TrimSpace(connector[adxDatabaseKey]),
		TenantID:     strings.TrimSpace(connector[azureTenantIDKey]),
		ClientID:     strings.TrimSpace(connector[azureClientIDKey]),
		ClientSecret: connector[azureClientSecretKey],
	}
}

// ApplyADXCredentialOverrides overlays AWS Secrets Manager fields onto an ADXConfig.
// azure_client_secret is the only confidential ADX attribute in backend-service.
func ApplyADXCredentialOverrides(cfg *ADXConfig, credentialOverrides map[string]string) {
	if cfg == nil {
		return
	}
	for k, v := range credentialOverrides {
		switch k {
		case azureTenantIDKey:
			cfg.TenantID = strings.TrimSpace(v)
		case azureClientIDKey:
			cfg.ClientID = strings.TrimSpace(v)
		case azureClientSecretKey:
			cfg.ClientSecret = v
		}
	}
}

// Validate reports missing fields using the same wording as backend-service AdxKqlSupport.
func (c *ADXConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("adx configuration is required")
	}
	if c.ClusterURI == "" {
		return fmt.Errorf("adx_cluster_uri is required in connector configuration")
	}
	if err := ValidateADXClusterURI(c.ClusterURI); err != nil {
		return err
	}
	if c.Database == "" {
		return fmt.Errorf("adx_database is required in connector configuration")
	}
	if c.TenantID == "" {
		return fmt.Errorf("azure_tenant_id is required")
	}
	if c.ClientID == "" {
		return fmt.Errorf("azure_client_id is required")
	}
	if c.ClientSecret == "" {
		return fmt.Errorf("azure_client_secret is required")
	}
	return nil
}
