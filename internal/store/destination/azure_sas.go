package destination

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

// GenerateBlobReadSASURL returns a time-limited HTTPS URL with read permission for a blob.
func GenerateBlobReadSASURL(ctx context.Context, cfg *AzureBlobConfig, container, blobName string, expiry time.Duration) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("azure blob config is required")
	}
	if container == "" || blobName == "" {
		return "", fmt.Errorf("container and blob name are required")
	}
	if expiry <= 0 {
		expiry = 24 * time.Hour
	}

	m := cfg.ToClientConfigMap()
	authType := m["azure_blob_auth_type"]
	if authType == "" && m["azure_blob_storage_account_connection_string"] != "" {
		authType = "AUTH_CONNECTION_STRING"
	}

	expiresAt := time.Now().UTC().Add(expiry)
	permissions := sas.BlobPermissions{Read: true}

	switch authType {
	case "AUTH_CONNECTION_STRING":
		conn := m["azure_blob_storage_account_connection_string"]
		if conn == "" {
			return "", fmt.Errorf("azure blob connection string is empty")
		}
		bbClient, err := blockblob.NewClientFromConnectionString(conn, container, blobName, nil)
		if err != nil {
			return "", fmt.Errorf("create block blob client: %w", err)
		}
		return bbClient.GetSASURL(permissions, expiresAt, nil)
	case "AUTH_SERVICE_PRINCIPAL":
		accountName := m["azure_blob_storage_account_name"]
		tenantID := m["azure_blob_tenant_id"]
		clientID := m["azure_blob_client_id"]
		clientSecret := m["azure_blob_client_secret"]
		if accountName == "" || tenantID == "" || clientID == "" || clientSecret == "" {
			return "", fmt.Errorf("service principal azure blob credentials are incomplete")
		}
		adCred, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
		if err != nil {
			return "", fmt.Errorf("create service principal credential: %w", err)
		}
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
		serviceClient, err := service.NewClient(serviceURL, adCred, nil)
		if err != nil {
			return "", fmt.Errorf("create azure service client: %w", err)
		}
		now := time.Now().UTC()
		keyInfo := service.KeyInfo{
			Start:  strPtr(now.Add(-5 * time.Minute).Format(time.RFC3339)),
			Expiry: strPtr(expiresAt.Format(time.RFC3339)),
		}
		udc, err := serviceClient.GetUserDelegationCredential(ctx, keyInfo, nil)
		if err != nil {
			return "", fmt.Errorf("get user delegation credential: %w", err)
		}
		signatureValues := sas.BlobSignatureValues{
			Version:       sas.Version,
			Protocol:      sas.ProtocolHTTPS,
			StartTime:     now.Add(-5 * time.Minute),
			ExpiryTime:    expiresAt,
			Permissions:   permissions.String(),
			ContainerName: container,
			BlobName:      blobName,
		}
		query, err := signatureValues.SignWithUserDelegation(udc)
		if err != nil {
			return "", fmt.Errorf("sign blob sas: %w", err)
		}
		return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s", accountName, container, blobName, query.Encode()), nil
	default:
		return "", fmt.Errorf("unsupported azure blob auth type for SAS: %s", authType)
	}
}

func strPtr(s string) *string { return &s }
