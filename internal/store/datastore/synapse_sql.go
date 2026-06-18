package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/google/uuid"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
	"gorm.io/gorm"
)

const (
	azureSynapseWorkspaceKey   = "azure_synapse_workspace"
	azureSynapseDatabaseKey    = "azure_synapse_database"
	azureSynapseSQLUsernameKey = "azure_synapse_sql_username"
	azureSynapseSQLPasswordKey = "azure_synapse_sql_password"
)

// LoadSynapseSQLConfig resolves JDBC settings from search_data_store, linked destination,
// external store secret, and data set search_configuration (mirrors backend-service).
func LoadSynapseSQLConfig(ctx context.Context, db *gorm.DB, dataStoreID, dataSetID, tenantID uuid.UUID) (*SynapseSQLConfig, error) {
	var row dataStoreRow
	err := db.WithContext(ctx).Raw(
		"SELECT id, type, destination_id, configuration FROM search_data_store WHERE id = ? AND tenant_id = ? LIMIT 1",
		dataStoreID, tenantID,
	).Scan(&row).Error
	if err != nil {
		return nil, fmt.Errorf("search_data_store not found: %w", err)
	}

	var storeCfg dataStoreConfiguration
	if row.Configuration != "" {
		if err := json.Unmarshal([]byte(row.Configuration), &storeCfg); err != nil {
			return nil, fmt.Errorf("failed to parse search_data_store configuration: %w", err)
		}
	}

	dsSyn, err := loadDatasetSynapseConfig(ctx, db, dataSetID, tenantID)
	if err != nil {
		return nil, err
	}

	storeType := strings.ToUpper(row.Type)
	switch storeType {
	case StoreTypeExternalStorage:
		return resolveExternalSynapseSQL(ctx, db, dataStoreID, tenantID, storeCfg, dsSyn)
	default:
		return resolveDestinationSynapseSQL(ctx, db, row.DestinationID, tenantID, storeCfg, dsSyn)
	}
}

func loadDatasetSynapseConfig(ctx context.Context, db *gorm.DB, dataSetID, tenantID uuid.UUID) (*datasetSynapseConfiguration, error) {
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
	return cfg.AzureSynapseConfiguration, nil
}

func resolveExternalSynapseSQL(
	ctx context.Context,
	db *gorm.DB,
	dataStoreID, tenantID uuid.UUID,
	storeCfg dataStoreConfiguration,
	dsSyn *datasetSynapseConfiguration,
) (*SynapseSQLConfig, error) {
	if storeCfg.ExternalSearchDataStoreConfiguration == nil {
		return nil, fmt.Errorf("external search data store configuration is required")
	}
	ext := storeCfg.ExternalSearchDataStoreConfiguration
	if strings.ToUpper(ext.ExternalSearchProvider) != ExternalProviderAzureBlob {
		return nil, fmt.Errorf("synapse export requires AZURE_BLOB external provider")
	}

	merged := cloneStringMap(ext.ConnectorConfig)
	if ext.SecretID != "" {
		overrides, err := destination.ResolveCredentialOverrides(ctx, db, ext.SecretID, dataStoreID, tenantID)
		if err != nil {
			return nil, err
		}
		merged = mergeStringMaps(merged, overrides)
	}

	cfg := &SynapseSQLConfig{
		Workspace:   strings.TrimSpace(dsSyn.Workspace),
		Database:    strings.TrimSpace(dsSyn.Database),
		SqlUsername: strings.TrimSpace(merged[azureSynapseSQLUsernameKey]),
		SqlPassword: trimTrailingWhitespace(merged[azureSynapseSQLPasswordKey]),
	}
	return validateSynapseSQLConfig(cfg)
}

func resolveDestinationSynapseSQL(
	ctx context.Context,
	db *gorm.DB,
	destinationID *uuid.UUID,
	tenantID uuid.UUID,
	storeCfg dataStoreConfiguration,
	dsSyn *datasetSynapseConfiguration,
) (*SynapseSQLConfig, error) {
	_ = dsSyn
	if storeCfg.AzureSynapseConfiguration == nil {
		return nil, fmt.Errorf("azure synapse configuration is required on search_data_store")
	}
	storeSyn := storeCfg.AzureSynapseConfiguration
	cfg := &SynapseSQLConfig{
		Workspace:   strings.TrimSpace(storeSyn.Workspace),
		Database:    strings.TrimSpace(storeSyn.Database),
		SqlUsername: strings.TrimSpace(storeSyn.SqlUsername),
		SqlPassword: trimTrailingWhitespace(storeSyn.SqlPassword),
	}

	// Fall back to linked destination secret only when store SQL creds are missing.
	if destinationID != nil && (strings.TrimSpace(cfg.SqlUsername) == "" || cfg.SqlPassword == "") {
		merged, err := destination.LoadMergedConfiguration(ctx, db, *destinationID, tenantID)
		if err != nil {
			return nil, err
		}
		cfg.SqlUsername = firstNonEmptyStr(cfg.SqlUsername, merged[azureSynapseSQLUsernameKey])
		cfg.SqlPassword = firstNonEmptyStr(cfg.SqlPassword, trimTrailingWhitespace(merged[azureSynapseSQLPasswordKey]))
	}

	return validateSynapseSQLConfig(cfg)
}

func validateSynapseSQLConfig(cfg *SynapseSQLConfig) (*SynapseSQLConfig, error) {
	if cfg.Workspace == "" || cfg.Database == "" {
		return nil, fmt.Errorf("synapse workspace and database are required")
	}
	if cfg.SqlUsername == "" || cfg.SqlPassword == "" {
		return nil, fmt.Errorf("synapse SQL credentials are required")
	}
	return cfg, nil
}

func trimTrailingWhitespace(s string) string {
	return strings.TrimRight(s, " \t\r\n")
}

func synapseMSDSNConfig(workspace, database, username, credential string) msdsn.Config {
	return msdsn.Config{
		Host:       fmt.Sprintf("%s-ondemand.sql.azuresynapse.net", workspace),
		Port:       1433,
		Database:   database,
		User:       username,
		Password:   trimTrailingWhitespace(credential),
		Encryption: msdsn.EncryptionRequired,
		Parameters: map[string]string{
			"host name in certificate": "*.database.windows.net",
		},
	}
}

// OpenSynapseSQL opens a Synapse serverless pool connection without building a loggable DSN string.
func OpenSynapseSQL(ctx context.Context, workspace, database, username, credential string) (*sql.DB, error) {
	connector := mssql.NewConnectorConfig(synapseMSDSNConfig(workspace, database, username, credential))
	db := sql.OpenDB(connector)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// BuildSynapseConnectionString builds a go-mssqldb URL DSN for tests only; do not log the return value.
func BuildSynapseConnectionString(workspace, database, username, credential string) string {
	return synapseMSDSNConfig(workspace, database, username, credential).URL().String()
}

func firstNonEmptyStr(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeStringMaps(base, overlay map[string]string) map[string]string {
	out := cloneStringMap(base)
	for k, v := range overlay {
		out[k] = v
	}
	return out
}
