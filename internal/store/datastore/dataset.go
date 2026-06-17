package datastore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type SynapseExportMetadata struct {
	DataSourceName string
	Database       string
}

// Matches SearchConfiguration JSON stored in search_data_set.search_configuration (backend-service).
type datasetSearchConfiguration struct {
	AzureSynapseConfiguration *datasetSynapseConfiguration `json:"azureSynapseConfiguration"`
}

type datasetSynapseConfiguration struct {
	Database       string `json:"database"`
	DataSourceName string `json:"dataSourceName"`
}

func LoadSynapseExportMetadata(ctx context.Context, db *gorm.DB, dataSetID, tenantID uuid.UUID) (*SynapseExportMetadata, error) {
	var configJSON string
	err := db.WithContext(ctx).Raw(
		"SELECT search_configuration FROM search_data_set WHERE id = ? AND tenant_id = ? LIMIT 1",
		dataSetID, tenantID,
	).Scan(&configJSON).Error
	if err != nil {
		return nil, fmt.Errorf("search_data_set not found: %w", err)
	}
	if configJSON == "" {
		return nil, fmt.Errorf("search_data_set search_configuration is empty for %s", dataSetID)
	}

	var cfg datasetSearchConfiguration
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse search_data_set search_configuration: %w", err)
	}
	if cfg.AzureSynapseConfiguration == nil {
		return nil, fmt.Errorf("azure synapse configuration missing on data set %s", dataSetID)
	}
	syn := cfg.AzureSynapseConfiguration
	return &SynapseExportMetadata{
		DataSourceName: syn.DataSourceName,
		Database:       syn.Database,
	}, nil
}
