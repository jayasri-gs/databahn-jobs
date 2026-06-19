package destination

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func NewAzureBlobClient(cfg *AzureBlobConfig) (*azblob.Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("azure blob config is required")
	}
	m := cfg.ToClientConfigMap()
	authType := m["azure_blob_auth_type"]
	if authType == "" && m["azure_blob_storage_account_connection_string"] != "" {
		authType = "AUTH_CONNECTION_STRING"
	}

	switch authType {
	case "AUTH_CONNECTION_STRING":
		conn := m["azure_blob_storage_account_connection_string"]
		if conn == "" {
			return nil, fmt.Errorf("azure blob connection string is empty")
		}
		return azblob.NewClientFromConnectionString(conn, nil)
	case "AUTH_SERVICE_PRINCIPAL":
		accountName := m["azure_blob_storage_account_name"]
		tenantID := m["azure_blob_tenant_id"]
		clientID := m["azure_blob_client_id"]
		clientSecret := m["azure_blob_client_secret"]
		if accountName == "" || tenantID == "" || clientID == "" || clientSecret == "" {
			return nil, fmt.Errorf("service principal azure blob credentials are incomplete")
		}
		cred, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create service principal credential: %w", err)
		}
		return azblob.NewClient(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName), cred, nil)
	default:
		return nil, fmt.Errorf("unsupported azure blob auth type: %s", authType)
	}
}
