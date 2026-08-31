package destination

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	DestTypeAzureDataExplorer = "AZURE_DATA_EXPLORER"

	// Pipeline ADX destination configuration keys, mirroring backend-service
	// AdxDestinationConfigMapper.
	pipelineADXClusterEndpointURL = "adx_cluster_endpoint_url"
	pipelineADXDatabaseName       = "adx_database_name"
	pipelineADXTenantID           = "adx_tenant_id"
	pipelineADXClientID           = "adx_client_id"
	pipelineADXClientSecret       = "adx_client_secret"
)

// LoadPipelineADXConfig resolves cluster credentials for a pipeline AZURE_DATA_EXPLORER
// destination (DATABAHN_DESTINATION search stores). Credentials are read live from the
// destination — including rotated secrets — rather than from a connector snapshot.
func LoadPipelineADXConfig(ctx context.Context, db *gorm.DB, destID, tenantID uuid.UUID) (*ADXConfig, error) {
	merged, err := LoadMergedConfiguration(ctx, db, destID, tenantID)
	if err != nil {
		return nil, err
	}
	cfg, err := pipelineADXConfig(merged)
	if err != nil {
		return nil, fmt.Errorf("pipeline ADX destination %s: %w", destID, err)
	}
	// The external-store path validates through loadADXConfig; this one had no check at all,
	// so the cluster endpoint reached the HTTP client unvalidated.
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("pipeline ADX destination %s: %w", destID, err)
	}
	return cfg, nil
}

// pipelineADXConfig maps pipeline destination keys onto the cluster settings the export
// worker needs (mirrors backend-service AdxDestinationConfigMapper.toConnectorConfig).
func pipelineADXConfig(config map[string]string) (*ADXConfig, error) {
	if len(config) == 0 {
		return nil, fmt.Errorf("Azure Data Explorer pipeline destination configuration is empty")
	}
	clusterURI, err := requirePipelineADXConfig(config, pipelineADXClusterEndpointURL)
	if err != nil {
		return nil, err
	}
	database, err := requirePipelineADXConfig(config, pipelineADXDatabaseName)
	if err != nil {
		return nil, err
	}
	tenantID, err := requirePipelineADXConfig(config, pipelineADXTenantID)
	if err != nil {
		return nil, err
	}
	clientID, err := requirePipelineADXConfig(config, pipelineADXClientID)
	if err != nil {
		return nil, err
	}
	clientSecret, err := requirePipelineADXConfig(config, pipelineADXClientSecret)
	if err != nil {
		return nil, err
	}
	return &ADXConfig{
		ClusterURI:   clusterURI,
		Database:     database,
		TenantID:     tenantID,
		ClientID:     clientID,
		ClientSecret: clientSecret,
	}, nil
}

func requirePipelineADXConfig(config map[string]string, key string) (string, error) {
	value := strings.TrimSpace(config[key])
	if value == "" {
		return "", fmt.Errorf("%s is required on the ADX destination", key)
	}
	return value, nil
}
