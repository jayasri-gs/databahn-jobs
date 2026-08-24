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
	QueryEngineAthena    = "ATHENA"
	QueryEngineSynapse   = "SYNAPSE"
	QueryEngineKustoADX  = "KUSTO_ADX"
	QueryEngineKustoLAW  = "KUSTO_LAW"
	QueryEngineKustoLake = "KUSTO_LAKE"

	StoreTypeDatabahnDestination = "DATABAHN_DESTINATION"
	StoreTypeDatabahnInsights    = "DATABAHN_INSIGHTS"
	StoreTypeDatabahnStorage     = "DATABAHN_STORAGE"
	StoreTypeExternalStorage     = "EXTERNAL_STORAGE"
	StoreTypeDerivedDatastore    = "DERIVED_DATASTORE"

	DestTypeS3                   = "S3"
	DestTypeS3Parquet            = "S3_PARQUET"
	DestTypeAzureBlob            = "AZURE_BLOB"
	DestTypeAWSSecurityLake      = "AWS_SECURITY_LAKE"
	DestTypeAzureDataExplorer    = "AZURE_DATA_EXPLORER"
	ExternalProviderS3           = "S3"
	ExternalProviderSecurityLake = "SECURITY_LAKE"
	ExternalProviderAzureBlob    = "AZURE_BLOB"
	ExternalProviderADX          = "AZURE_DATA_EXPLORER"
	ExternalProviderSentinel     = "AZURE_SENTINEL"
)

type ExportDataStore struct {
	ID                     uuid.UUID
	Type                   string
	QueryEngine            string
	ExternalSearchProvider string
	StagingS3              *destination.S3Config
	StagingBlob            *destination.AzureBlobConfig
	SynapseSQL             *SynapseSQLConfig
	ADX                    *destination.ADXConfig
	Sentinel               *destination.SentinelConfig
}

type SynapseSQLConfig struct {
	Workspace   string
	Database    string
	SqlUsername string
	SqlPassword string
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

// DeriveQueryEngine maps a data store onto its export engine. storageTier is read only for
// Sentinel stores, where it separates the analytics and lake tiers.
func DeriveQueryEngine(storeType, linkedDestType, externalProvider, storageTier string) string {
	switch storeType {
	case StoreTypeDatabahnDestination:
		switch linkedDestType {
		case DestTypeS3, DestTypeS3Parquet, DestTypeAWSSecurityLake:
			return QueryEngineAthena
		case DestTypeAzureBlob:
			return QueryEngineSynapse
		case DestTypeAzureDataExplorer:
			return QueryEngineKustoADX
		}
	case StoreTypeDatabahnInsights, StoreTypeDatabahnStorage:
		return QueryEngineAthena
	case StoreTypeExternalStorage, StoreTypeDerivedDatastore:
		switch externalProvider {
		case ExternalProviderS3, ExternalProviderSecurityLake:
			return QueryEngineAthena
		case ExternalProviderAzureBlob:
			return QueryEngineSynapse
		case ExternalProviderADX:
			return QueryEngineKustoADX
		case ExternalProviderSentinel:
			// The connectorConfig's storage_tier decides which Sentinel tier this is; the
			// two engines use different endpoints, scopes and response formats.
			if destination.NormalizeSentinelTier(storageTier) == destination.SentinelTierLake {
				return QueryEngineKustoLake
			}
			return QueryEngineKustoLAW
		}
	}
	return ""
}

// IsExternalAthenaProvider reports whether the store uses Athena over an external/derived sink.
// Legacy stores may declare provider=S3 with connectorConfig.security_lake=true.
func IsExternalAthenaProvider(provider string, connector map[string]string) bool {
	switch strings.ToUpper(strings.TrimSpace(provider)) {
	case ExternalProviderS3, ExternalProviderSecurityLake:
		return true
	}
	if connector != nil && strings.EqualFold(strings.TrimSpace(connector["security_lake"]), "true") {
		return true
	}
	return false
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
	var connector map[string]string
	var secretID string
	if storeCfg.ExternalSearchDataStoreConfiguration != nil {
		ext := storeCfg.ExternalSearchDataStoreConfiguration
		externalProvider = strings.ToUpper(ext.ExternalSearchProvider)
		connector = ext.ConnectorConfig
		secretID = ext.SecretID
		// Legacy Security Lake stores may still advertise provider=S3.
		if externalProvider == ExternalProviderS3 &&
			strings.EqualFold(strings.TrimSpace(connector["security_lake"]), "true") {
			externalProvider = ExternalProviderSecurityLake
		}
	}

	linkedDestType := ""
	storeType := strings.ToUpper(row.Type)
	result := &ExportDataStore{
		ID:                     row.ID,
		Type:                   storeType,
		ExternalSearchProvider: externalProvider,
	}

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
		case DestTypeAWSSecurityLake:
			s3Cfg, err := destination.LoadPipelineSecurityLakeS3Config(ctx, db, *row.DestinationID, tenantID)
			if err != nil {
				return nil, err
			}
			result.StagingS3 = s3Cfg
			result.ExternalSearchProvider = ExternalProviderSecurityLake
		case DestTypeAzureBlob:
			blobCfg, err := destination.LoadAzureBlobConfig(ctx, db, *row.DestinationID, tenantID)
			if err != nil {
				return nil, err
			}
			result.StagingBlob = blobCfg
		case DestTypeAzureDataExplorer:
			adxCfg, err := destination.LoadPipelineADXConfig(ctx, db, *row.DestinationID, tenantID)
			if err != nil {
				return nil, err
			}
			result.ADX = adxCfg
			result.ExternalSearchProvider = ExternalProviderADX
		}
	}

	result.QueryEngine = DeriveQueryEngine(storeType, linkedDestType, externalProvider, connector["storage_tier"])
	if result.QueryEngine == "" {
		return nil, fmt.Errorf("unsupported search_data_store: type=%s dest=%s provider=%s", storeType, linkedDestType, externalProvider)
	}

	if result.QueryEngine == QueryEngineAthena &&
		(storeType == StoreTypeExternalStorage || storeType == StoreTypeDerivedDatastore) &&
		IsExternalAthenaProvider(externalProvider, connector) {
		staging, err := loadExternalAthenaStaging(ctx, db, dataStoreID, tenantID, secretID, connector)
		if err != nil {
			return nil, err
		}
		result.StagingS3 = staging
	}

	if result.QueryEngine == QueryEngineKustoADX && result.ADX == nil {
		adxCfg, err := loadADXConfig(ctx, db, dataStoreID, tenantID, secretID, connector)
		if err != nil {
			return nil, err
		}
		result.ADX = adxCfg
	}

	if result.QueryEngine == QueryEngineKustoLAW {
		sentinelCfg, err := loadSentinelConfig(ctx, db, dataStoreID, tenantID, secretID, connector)
		if err != nil {
			return nil, err
		}
		result.Sentinel = sentinelCfg
	}

	if result.QueryEngine == QueryEngineSynapse && storeCfg.AzureSynapseConfiguration != nil {
		s := storeCfg.AzureSynapseConfiguration
		result.SynapseSQL = &SynapseSQLConfig{
			Workspace:   s.Workspace,
			Database:    s.Database,
			SqlUsername: s.SqlUsername,
			SqlPassword: s.SqlPassword,
		}
	}

	return result, nil
}

func loadExternalAthenaStaging(
	ctx context.Context,
	db *gorm.DB,
	dataStoreID, tenantID uuid.UUID,
	secretID string,
	connector map[string]string,
) (*destination.S3Config, error) {
	staging := destination.S3ConfigFromExternalConnector(connector)
	if staging == nil {
		return nil, fmt.Errorf("external Athena store %s has empty connectorConfig", dataStoreID)
	}
	if secretID != "" {
		overrides, err := destination.ResolveCredentialOverrides(ctx, db, secretID, dataStoreID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("resolve external store secret: %w", err)
		}
		destination.ApplyS3CredentialOverrides(staging, overrides)
	}
	return staging, nil
}

// loadADXConfig resolves Azure Data Explorer cluster credentials for an EXTERNAL_STORAGE
// store, overlaying azure_client_secret from Secrets Manager when the store references one.
// Pipeline (DATABAHN_DESTINATION) ADX stores take their credentials from the linked
// destination instead — see destination.LoadPipelineADXConfig.
func loadADXConfig(
	ctx context.Context,
	db *gorm.DB,
	dataStoreID, tenantID uuid.UUID,
	secretID string,
	connector map[string]string,
) (*destination.ADXConfig, error) {
	cfg := destination.ADXConfigFromExternalConnector(connector)
	if cfg == nil {
		return nil, fmt.Errorf("ADX store %s has empty connectorConfig", dataStoreID)
	}
	if secretID != "" {
		overrides, err := destination.ResolveCredentialOverrides(ctx, db, secretID, dataStoreID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("resolve ADX store secret: %w", err)
		}
		destination.ApplyADXCredentialOverrides(cfg, overrides)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("ADX store %s: %w", dataStoreID, err)
	}
	return cfg, nil
}

// loadSentinelConfig resolves Microsoft Sentinel workspace credentials for an
// EXTERNAL_STORAGE store, overlaying azure_client_secret from Secrets Manager when the store
// references one. Mirrors loadADXConfig — the two providers share an Entra app credential
// shape and the same single confidential attribute.
func loadSentinelConfig(
	ctx context.Context,
	db *gorm.DB,
	dataStoreID, tenantID uuid.UUID,
	secretID string,
	connector map[string]string,
) (*destination.SentinelConfig, error) {
	cfg := destination.SentinelConfigFromExternalConnector(connector)
	if cfg == nil {
		return nil, fmt.Errorf("Sentinel store %s has empty connectorConfig", dataStoreID)
	}
	if secretID != "" {
		overrides, err := destination.ResolveCredentialOverrides(ctx, db, secretID, dataStoreID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("resolve Sentinel store secret: %w", err)
		}
		destination.ApplySentinelCredentialOverrides(cfg, overrides)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("Sentinel store %s: %w", dataStoreID, err)
	}
	return cfg, nil
}
