package destination

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	DestTypeAzureSentinel         = "AZURE_SENTINEL"
	DestTypeAzureSentinelDataLake = "AZURE_SENTINEL_DATA_LAKE"

	// Pipeline Sentinel destination configuration keys, mirroring backend-service
	// AzureSentinelDestination. workspace_id and workspace_name are shared with the
	// external-store connector shape and declared in sentinel_config.go.
	pipelineSentinelTenantID     = "azure_sentinel_auth_azure_tenant_id"
	pipelineSentinelClientID     = "azure_sentinel_auth_azure_client_id"
	pipelineSentinelClientSecret = "azure_sentinel_auth_azure_client_secret"
)

// SentinelTierForDestinationType maps a pipeline destination type onto the Sentinel tier it
// queries. An external Sentinel store carries storage_tier in its connectorConfig; a pipeline
// store has no connectorConfig of its own, so the destination type is the only signal.
// Mirrors backend-service SentinelDestinationConfigMapper.storageTierFor.
func SentinelTierForDestinationType(destType string) string {
	switch strings.ToUpper(strings.TrimSpace(destType)) {
	case DestTypeAzureSentinel:
		return SentinelTierAnalytics
	case DestTypeAzureSentinelDataLake:
		return SentinelTierLake
	}
	return ""
}

// LoadPipelineSentinelConfig resolves workspace credentials for a pipeline AZURE_SENTINEL or
// AZURE_SENTINEL_DATA_LAKE destination (DATABAHN_DESTINATION search stores). Credentials are
// read live from the destination — including rotated secrets — rather than from a connector
// snapshot. Mirrors LoadPipelineADXConfig.
func LoadPipelineSentinelConfig(ctx context.Context, db *gorm.DB, destID, tenantID uuid.UUID, destType string) (*SentinelConfig, error) {
	tier := SentinelTierForDestinationType(destType)
	if tier == "" {
		return nil, fmt.Errorf("pipeline Sentinel destination %s: unsupported destination type %s", destID, destType)
	}
	merged, err := LoadMergedConfiguration(ctx, db, destID, tenantID)
	if err != nil {
		return nil, err
	}
	cfg, err := pipelineSentinelConfig(merged, tier)
	if err != nil {
		return nil, fmt.Errorf("pipeline Sentinel destination %s: %w", destID, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("pipeline Sentinel destination %s: %w", destID, err)
	}
	return cfg, nil
}

// pipelineSentinelConfig maps pipeline destination keys onto the workspace settings the export
// worker needs (mirrors backend-service SentinelDestinationConfigMapper.toConnectorConfig).
func pipelineSentinelConfig(config map[string]string, tier string) (*SentinelConfig, error) {
	if len(config) == 0 {
		return nil, fmt.Errorf("Sentinel pipeline destination configuration is empty")
	}
	workspaceID, err := requirePipelineSentinelConfig(config, sentinelWorkspaceIDKey)
	if err != nil {
		return nil, err
	}
	workspaceName, err := requirePipelineSentinelConfig(config, sentinelWorkspaceNameKey)
	if err != nil {
		return nil, err
	}
	tenantID, err := requirePipelineSentinelConfig(config, pipelineSentinelTenantID)
	if err != nil {
		return nil, err
	}
	clientID, err := requirePipelineSentinelConfig(config, pipelineSentinelClientID)
	if err != nil {
		return nil, err
	}
	clientSecret, err := requirePipelineSentinelConfig(config, pipelineSentinelClientSecret)
	if err != nil {
		return nil, err
	}
	return &SentinelConfig{
		WorkspaceID:   workspaceID,
		WorkspaceName: workspaceName,
		StorageTier:   tier,
		TenantID:      tenantID,
		ClientID:      clientID,
		ClientSecret:  clientSecret,
	}, nil
}

func requirePipelineSentinelConfig(config map[string]string, key string) (string, error) {
	value := strings.TrimSpace(config[key])
	if value == "" {
		return "", fmt.Errorf("%s is required on the Sentinel destination", key)
	}
	return value, nil
}
