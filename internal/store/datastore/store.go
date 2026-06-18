package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	QueryEngineAthena  = "ATHENA"
	QueryEngineSynapse = "SYNAPSE"

	StoreTypeDatabahnDestination = "DATABAHN_DESTINATION"
	StoreTypeDatabahnInsights    = "DATABAHN_INSIGHTS"
	StoreTypeDatabahnStorage     = "DATABAHN_STORAGE"
	StoreTypeExternalStorage     = "EXTERNAL_STORAGE"

	DestTypeS3                   = "S3"
	DestTypeS3Parquet            = "S3_PARQUET"
	DestTypeAzureBlob            = "AZURE_BLOB"
	ExternalProviderS3           = "S3"
	ExternalProviderSecurityLake = "SECURITY_LAKE"
	ExternalProviderAzureBlob    = "AZURE_BLOB"
)

type ExportDataStore struct {
	ID          uuid.UUID
	Type        string
	QueryEngine string
	StagingS3   *destination.S3Config
	StagingBlob *destination.AzureBlobConfig
	SynapseSQL  *SynapseSQLConfig
}

type SynapseSQLConfig struct {
	ConnectionString string
	Workspace        string
	Database         string
	SqlUsername      string
	SqlPassword      string
}

type dataStoreRow struct {
	ID            uuid.UUID  `gorm:"column:id"`
	Type          string     `gorm:"column:type"`
	DestinationID *uuid.UUID `gorm:"column:destination_id"`
	Configuration string     `gorm:"column:configuration"`
}

type dataStoreConfiguration struct {
	AzureSynapseConfiguration            *synapseSQLConfigJSON    `json:"azureSynapseConfiguration"`
	ExternalSearchDataStoreConfiguration *externalStoreConfigJSON `json:"externalSearchDataStoreConfiguration"`
}

type synapseSQLConfigJSON struct {
	Workspace   string `json:"workspace"`
	Database    string `json:"database"`
	SqlUsername string `json:"sqlUsername"`
	SqlPassword string `json:"sqlPassword"`
}

type externalStoreConfigJSON struct {
	ExternalSearchProvider string            `json:"externalSearchProvider"`
	SecretID               string            `json:"secretId"`
	ConnectorConfig        map[string]string `json:"connectorConfig"`
}

func DeriveQueryEngine(storeType, linkedDestType, externalProvider string) string {
	switch storeType {
	case StoreTypeDatabahnDestination:
		switch linkedDestType {
		case DestTypeS3, DestTypeS3Parquet:
			return QueryEngineAthena
		case DestTypeAzureBlob:
			return QueryEngineSynapse
		}
	case StoreTypeDatabahnInsights, StoreTypeDatabahnStorage:
		return QueryEngineAthena
	case StoreTypeExternalStorage:
		switch externalProvider {
		case ExternalProviderS3, ExternalProviderSecurityLake:
			return QueryEngineAthena
		case ExternalProviderAzureBlob:
			return QueryEngineSynapse
		}
	}
	return ""
}

func LoadExportDataStore(ctx context.Context, db *gorm.DB, dataStoreID, tenantID uuid.UUID) (*ExportDataStore, error) {
	var row dataStoreRow
	err := db.WithContext(ctx).Raw(
		"SELECT id, type, destination_id, configuration FROM search_data_store WHERE id = ? AND tenant_id = ? LIMIT 1",
		dataStoreID, tenantID,
	).Scan(&row).Error
	if err != nil {
		return nil, fmt.Errorf("search_data_store not found: %w", err)
	}
	if row.Type == "" {
		return nil, fmt.Errorf("search_data_store type is empty for %s", dataStoreID)
	}

	var storeCfg dataStoreConfiguration
	if row.Configuration != "" {
		if err := json.Unmarshal([]byte(row.Configuration), &storeCfg); err != nil {
			return nil, fmt.Errorf("failed to parse search_data_store configuration: %w", err)
		}
	}

	externalProvider := ""
	if storeCfg.ExternalSearchDataStoreConfiguration != nil {
		externalProvider = strings.ToUpper(storeCfg.ExternalSearchDataStoreConfiguration.ExternalSearchProvider)
	}

	linkedDestType := ""
	storeType := strings.ToUpper(row.Type)
	result := &ExportDataStore{ID: row.ID, Type: storeType}

	if row.DestinationID != nil {
		var destType string
		err := db.WithContext(ctx).Raw(
			"SELECT destination_type FROM destination WHERE id = ? AND tenant_id = ? LIMIT 1",
			*row.DestinationID, tenantID,
		).Scan(&destType).Error
		if err != nil {
			return nil, fmt.Errorf("linked destination not found: %w", err)
		}
		linkedDestType = strings.ToUpper(destType)
		switch linkedDestType {
		case DestTypeS3, DestTypeS3Parquet:
			s3Cfg, err := destination.LoadS3Config(ctx, db, *row.DestinationID, tenantID)
			if err != nil {
				return nil, err
			}
			result.StagingS3 = s3Cfg
		case DestTypeAzureBlob:
			blobCfg, err := destination.LoadAzureBlobConfig(ctx, db, *row.DestinationID, tenantID)
			if err != nil {
				return nil, err
			}
			result.StagingBlob = blobCfg
		}
	}

	result.QueryEngine = DeriveQueryEngine(storeType, linkedDestType, externalProvider)
	if result.QueryEngine == "" {
		return nil, fmt.Errorf("unsupported search_data_store: type=%s dest=%s provider=%s", storeType, linkedDestType, externalProvider)
	}

	if result.QueryEngine == QueryEngineSynapse && storeCfg.AzureSynapseConfiguration != nil {
		s := storeCfg.AzureSynapseConfiguration
		result.SynapseSQL = &SynapseSQLConfig{
			Workspace:   s.Workspace,
			Database:    s.Database,
			SqlUsername: s.SqlUsername,
			SqlPassword: s.SqlPassword,
		}
		if s.Workspace != "" && s.Database != "" && s.SqlUsername != "" && s.SqlPassword != "" {
			result.SynapseSQL.ConnectionString = BuildSynapseConnectionString(s.Workspace, s.Database, s.SqlUsername, s.SqlPassword)
		}
	}

	return result, nil
}
