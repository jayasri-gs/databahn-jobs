package destination

import (
	"fmt"
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
