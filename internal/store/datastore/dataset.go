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

type datasetConfiguration struct {
	SearchConfiguration *datasetSearchConfiguration `json:"searchConfiguration"`
}

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
		"SELECT configuration FROM search_data_set WHERE id = ? AND tenant_id = ? LIMIT 1",
		dataSetID, tenantID,
	).Scan(&configJSON).Error
	if err != nil {
		return nil, fmt.Errorf("search_data_set not found: %w", err)
	}
	if configJSON == "" {
		return nil, fmt.Errorf("search_data_set configuration is empty for %s", dataSetID)
	}

	var cfg datasetConfiguration
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse search_data_set configuration: %w", err)
	}
	if cfg.SearchConfiguration == nil || cfg.SearchConfiguration.AzureSynapseConfiguration == nil {
		return nil, fmt.Errorf("azure synapse configuration missing on data set %s", dataSetID)
	}
	syn := cfg.SearchConfiguration.AzureSynapseConfiguration
	return &SynapseExportMetadata{
		DataSourceName: syn.DataSourceName,
		Database:       syn.Database,
	}, nil
}
