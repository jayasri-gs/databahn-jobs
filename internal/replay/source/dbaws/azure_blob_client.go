package dbaws

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
)

// azureBlobClientOptions returns client options that disable HTTP-level gzip
// auto-decompression. Vector dispenser blobs set Content-Encoding: gzip metadata;
// without this, the Go HTTP client transparently decompresses on download while
// the .log.gz filename still implies gzip bytes on disk.
func azureBlobClientOptions() *azblob.ClientOptions {
	return &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: &http.Client{
				Transport: &http.Transport{
					DisableCompression: true,
				},
			},
		},
	}
}

// GetAzureBlobClient builds an azblob.Client from replay AdditionalConfig key/value pairs.
func GetAzureBlobClient(config map[string]string) (*azblob.Client, error) {
	authType := config["azure_blob_auth_type"]
	// Legacy configs may omit azure_blob_auth_type when only a connection string is set.
	if authType == "" && config["azure_blob_storage_account_connection_string"] != "" {
		logger.GetLogger().Info("Auth type not set in config, using default auth type as fallback")
		authType = "AUTH_CONNECTION_STRING"
	}

	switch authType {
	case "AUTH_CONNECTION_STRING":
		logger.GetLogger().Info("Using connection string authentication")
		connString := config["azure_blob_storage_account_connection_string"]
		if connString == "" {
			return nil, fmt.Errorf("azure blob connection string is empty")
		}

		client, err := azblob.NewClientFromConnectionString(connString, azureBlobClientOptions())
		if err != nil {
			logger.GetLogger().Error("Failed to create Azure Blob client with connection string")
			return nil, err
		}
		return client, nil
	case "AUTH_USER_DELEGATED_SAS_KEY":
		logger.GetLogger().Info("Using User Delegated SAS Token authentication")
		return getClientWithUserDelegatedSAS(config)
	case "AUTH_SERVICE_PRINCIPAL":
		logger.GetLogger().Info("Using service principal authentication")
		return getClientWithServicePrincipal(config)
	default:
		return nil, fmt.Errorf("invalid authentication type: %s", authType)
	}
}

// getClientWithUserDelegatedSAS creates an Azure Blob Storage client using User Delegated SAS Token.
// Expiry duration is configurable via USER_DELEGATED_SAS_TOKEN_EXPIRY_MINUTES environment variable.
func getClientWithUserDelegatedSAS(config map[string]string) (*azblob.Client, error) {

	accountName := config["azure_blob_storage_account_name"]
	tenantId := config["azure_blob_tenant_id"]
	clientId := config["azure_blob_client_id"]
	clientSecret := config["azure_blob_client_secret"]
	containerName := config["azure_blob_container"]

	if accountName == "" || tenantId == "" || clientId == "" || clientSecret == "" || containerName == "" {
		return nil, fmt.Errorf("missing required Azure Blob credentials for AUTH_USER_DELEGATED_SAS_KEY authentication")
	}

	ctx := context.Background()

	cred, err := azidentity.NewClientSecretCredential(
		tenantId,
		clientId,
		clientSecret,
		nil,
	)
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to create Azure AD credential: %v", err))
		return nil, fmt.Errorf("failed to create Azure AD credential: %w", err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	serviceClient, err := service.NewClient(serviceURL, cred, nil)
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to create Azure Blob service client: %v", err))
		return nil, fmt.Errorf("failed to create Azure Blob service client: %w", err)
	}

	tokenExpiryDuration := userDelegatedSASTokenExpiryDuration()
	clockSkew := userDelegationSasClockSkew()
	now := time.Now().UTC()
	// Start slightly in the past so the first blob requests are not rejected when Azure
	// Storage time trails this host (AuthenticationFailed: Current < Start).
	keyStart := now.Add(-clockSkew)
	keyExpiry := now.Add(tokenExpiryDuration)

	logger.GetLogger().Info(fmt.Sprintf(
		"User Delegation Key expiry duration: %v, clock skew: %v",
		tokenExpiryDuration,
		clockSkew,
	))

	startTimeStr := keyStart.Format(time.RFC3339)
	expiryTimeStr := keyExpiry.Format(time.RFC3339)

	keyInfo := service.KeyInfo{
		Start:  &startTimeStr,
		Expiry: &expiryTimeStr,
	}

	userDelegationCred, err := serviceClient.GetUserDelegationCredential(ctx, keyInfo, nil)
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to get user delegation credential: %v", err))
		return nil, fmt.Errorf("failed to get user delegation credential: %w", err)
	}

	permissions := sas.ContainerPermissions{
		Read:   true,
		List:   true,
		Create: false,
		Write:  false,
		Delete: false,
		Add:    false,
	}

	signatureValues := sas.BlobSignatureValues{
		Version:       sas.Version,
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     keyStart,
		ExpiryTime:    keyExpiry,
		Permissions:   permissions.String(),
		ContainerName: containerName,
	}

	sasQueryParams, err := signatureValues.SignWithUserDelegation(userDelegationCred)
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to sign SAS with user delegation: %v", err))
		return nil, fmt.Errorf("failed to sign SAS with user delegation: %w", err)
	}

	serviceURLWithSAS := fmt.Sprintf("%s?%s", serviceURL, sasQueryParams.Encode())

	blobClient, err := azblob.NewClientWithNoCredential(serviceURLWithSAS, azureBlobClientOptions())
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to create Azure Blob client with SAS token: %v", err))
		return nil, fmt.Errorf("failed to create Azure Blob client with SAS token: %w", err)
	}

	logger.GetLogger().Info("Successfully created Azure Blob client with User Delegated SAS Token")
	return blobClient, nil
}

func userDelegatedSASTokenExpiryDuration() time.Duration {
	const envKey = "USER_DELEGATED_SAS_TOKEN_EXPIRY_MINUTES"
	const defaultMinutes = 1 * 24 * 60 // 1 day in minutes
	minutes := utils.GetEnvInt(envKey, defaultMinutes)
	return time.Duration(minutes) * time.Minute
}

// userDelegationSasClockSkew subtracts this duration from SAS / user-delegation key start times.
// Matches backend-service ReplayListFileService (USER_DELEGATED_SAS_CLOCK_SKEW_MINUTES).
func userDelegationSasClockSkew() time.Duration {
	const envKey = "USER_DELEGATED_SAS_CLOCK_SKEW_MINUTES"
	const defaultMinutes = 5
	minutes := utils.GetEnvInt(envKey, defaultMinutes)
	if minutes < 0 {
		minutes = defaultMinutes
	}
	return time.Duration(minutes) * time.Minute
}

func getClientWithServicePrincipal(config map[string]string) (*azblob.Client, error) {

	accountName := config["azure_blob_storage_account_name"]
	tenantId := config["azure_blob_tenant_id"]
	clientId := config["azure_blob_client_id"]
	clientSecret := config["azure_blob_client_secret"]

	if accountName == "" || tenantId == "" || clientId == "" || clientSecret == "" {
		return nil, fmt.Errorf("missing required Azure Blob credentials for AUTH_SERVICE_PRINCIPAL authentication")
	}

	cred, err := azidentity.NewClientSecretCredential(
		tenantId,
		clientId,
		clientSecret,
		nil,
	)
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to create Azure AD credential: %v", err))
		return nil, fmt.Errorf("failed to create Azure AD credential: %w", err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	serviceClient, err := azblob.NewClient(serviceURL, cred, azureBlobClientOptions())
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to create Azure Blob service client: %v", err))
		return nil, fmt.Errorf("failed to create Azure Blob service client: %w", err)
	}

	logger.GetLogger().Info("Successfully created Azure Blob client with Service principal")
	return serviceClient, nil
}
