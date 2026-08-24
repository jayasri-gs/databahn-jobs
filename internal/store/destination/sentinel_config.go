package destination

import (
	"fmt"
	"strings"
)

// Sentinel connector configuration keys, mirroring backend-service AzureConstants.
// The Entra app keys (azure_tenant_id / azure_client_id / azure_client_secret) are
// shared with ADX and declared in adx_config.go.
const (
	sentinelWorkspaceIDKey   = "workspace_id"
	sentinelWorkspaceNameKey = "workspace_name"
	sentinelStorageTierKey   = "storage_tier"
)

// Sentinel data store tiers. Only the analytics tier is exportable today; the lake tier
// has no query transport in the worker yet.
const (
	SentinelTierAnalytics = "ANALYTICS"
	SentinelTierLake      = "LAKE"
)

// SentinelConfig holds the credentials needed to run KQL against a Microsoft Sentinel
// workspace. WorkspaceName is carried for the lake tier only, where the KQL `db` field is
// `workspaceName-workspaceId` rather than the GUID.
type SentinelConfig struct {
	WorkspaceID   string
	WorkspaceName string
	StorageTier   string
	TenantID      string
	ClientID      string
	ClientSecret  string
}

// SentinelConfigFromExternalConnector maps an EXTERNAL_STORAGE connectorConfig onto a
// SentinelConfig. The client secret is normally blank here and supplied by
// ApplySentinelCredentialOverrides.
func SentinelConfigFromExternalConnector(connector map[string]string) *SentinelConfig {
	if len(connector) == 0 {
		return nil
	}
	return &SentinelConfig{
		WorkspaceID:   strings.TrimSpace(connector[sentinelWorkspaceIDKey]),
		WorkspaceName: strings.TrimSpace(connector[sentinelWorkspaceNameKey]),
		StorageTier:   NormalizeSentinelTier(connector[sentinelStorageTierKey]),
		TenantID:      strings.TrimSpace(connector[azureTenantIDKey]),
		ClientID:      strings.TrimSpace(connector[azureClientIDKey]),
		ClientSecret:  connector[azureClientSecretKey],
	}
}

// NormalizeSentinelTier upper-cases the configured tier, defaulting to ANALYTICS.
// backend-service treats a blank storage_tier as the analytics tier.
func NormalizeSentinelTier(tier string) string {
	t := strings.ToUpper(strings.TrimSpace(tier))
	if t == "" {
		return SentinelTierAnalytics
	}
	return t
}

// ApplySentinelCredentialOverrides overlays AWS Secrets Manager fields onto a SentinelConfig.
// azure_client_secret is the only confidential Sentinel attribute in backend-service.
func ApplySentinelCredentialOverrides(cfg *SentinelConfig, credentialOverrides map[string]string) {
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

// Validate reports missing fields using the same wording as backend-service
// LogAnalyticsKqlSupport, and rejects tiers the worker cannot export.
func (c *SentinelConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("sentinel configuration is required")
	}
	if c.WorkspaceID == "" {
		return fmt.Errorf("workspace_id is required in connector configuration")
	}
	switch NormalizeSentinelTier(c.StorageTier) {
	case SentinelTierAnalytics:
	case SentinelTierLake:
		// The lake KQL API addresses the workspace by a composite name, not the GUID, so a
		// lake store without workspace_name cannot be queried at all. Failing here names the
		// missing key; failing later surfaces as an opaque API error.
		if c.WorkspaceName == "" {
			return fmt.Errorf("workspace_name is required in connector configuration when storage_tier is LAKE")
		}
	default:
		return fmt.Errorf("unsupported Sentinel storage_tier: %s", c.StorageTier)
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

// LakeDatabase is the database identifier the Sentinel lake KQL API expects: the workspace
// name and its GUID joined by a hyphen, not the GUID alone. Mirrors backend-service
// SentinelLakeKqlSupport.requireLakeKqlDatabase.
func (c *SentinelConfig) LakeDatabase() string {
	if c == nil || c.WorkspaceName == "" || c.WorkspaceID == "" {
		return ""
	}
	return c.WorkspaceName + "-" + c.WorkspaceID
}
