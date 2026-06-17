package destination

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type AzureBlobConfig struct {
	AuthType         string
	ConnectionString string
	AccountName      string
	TenantID         string
	ClientID         string
	ClientSecret     string
	Container        string
}

func (c *AzureBlobConfig) ToClientConfigMap() map[string]string {
	return map[string]string{
		"azure_blob_auth_type":                         c.AuthType,
		"azure_blob_container":                         c.Container,
		"azure_blob_storage_account_connection_string": c.ConnectionString,
		"azure_blob_storage_account_name":              c.AccountName,
		"azure_blob_tenant_id":                         c.TenantID,
		"azure_blob_client_id":                         c.ClientID,
		"azure_blob_client_secret":                     c.ClientSecret,
	}
}

type azureBlobConfigRow struct {
	ID            uuid.UUID `gorm:"column:id"`
	TenantID      uuid.UUID `gorm:"column:tenant_id"`
	Configuration string    `gorm:"column:configuration"`
}

func parseAzureBlobConfigFromWrapper(wrapper ConfigWrapper, credentialOverrides map[string]string) (*AzureBlobConfig, error) {
	cfg := &AzureBlobConfig{
		AuthType:         wrapper.getString("azure_blob_auth_type"),
		ConnectionString: wrapper.getString("azure_blob_storage_account_connection_string"),
		AccountName:      wrapper.getString("azure_blob_storage_account_name"),
		TenantID:         wrapper.getString("azure_blob_tenant_id"),
		ClientID:         wrapper.getString("azure_blob_client_id"),
		ClientSecret:     wrapper.getString("azure_blob_client_secret"),
		Container:        wrapper.getString("azure_blob_container"),
	}

	for k, v := range credentialOverrides {
		switch k {
		case "azure_blob_storage_account_connection_string":
			cfg.ConnectionString = v
		case "azure_blob_storage_account_name":
			cfg.AccountName = v
		case "azure_blob_tenant_id":
			cfg.TenantID = v
		case "azure_blob_client_id":
			cfg.ClientID = v
		case "azure_blob_client_secret":
			cfg.ClientSecret = v
		}
	}

	if cfg.AuthType == "" && cfg.ConnectionString != "" {
		cfg.AuthType = "AUTH_CONNECTION_STRING"
	}
	if cfg.Container == "" {
		return nil, fmt.Errorf("azure blob container is required")
	}
	if cfg.AuthType == "" {
		return nil, fmt.Errorf("azure blob auth type is required")
	}

	return cfg, nil
}

func LoadAzureBlobConfig(ctx context.Context, db *gorm.DB, destID, tenantID uuid.UUID) (*AzureBlobConfig, error) {
	var dest azureBlobConfigRow
	err := db.WithContext(ctx).Raw(
		"SELECT id, tenant_id, configuration FROM destination WHERE id = ? AND tenant_id = ? LIMIT 1",
		destID, tenantID,
	).Scan(&dest).Error
	if err != nil {
		return nil, fmt.Errorf("destination not found: %w", err)
	}

	if dest.Configuration == "" {
		return nil, fmt.Errorf("destination configuration is empty for %s", destID)
	}

	var wrapper ConfigWrapper
	if err := json.Unmarshal([]byte(dest.Configuration), &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse destination configuration: %w", err)
	}

	var credentialOverrides map[string]string
	if wrapper.SecretID != "" {
		var err error
		credentialOverrides, err = ResolveCredentialOverrides(ctx, db, wrapper.SecretID, destID, dest.TenantID)
		if err != nil {
			return nil, err
		}
	}

	return parseAzureBlobConfigFromWrapper(wrapper, credentialOverrides)
}
