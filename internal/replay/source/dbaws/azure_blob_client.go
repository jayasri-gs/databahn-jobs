package dbaws

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
)

// GetAzureBlobClient builds an azblob.Client from replay AdditionalConfig key/value pairs.
func GetAzureBlobClient(config map[string]string) (*azblob.Client, error) {
	authType := config["azure_blob_auth_type"]

	switch authType {
	case "AUTH_CONNECTION_STRING":
		logger.GetLogger().Info("Using connection string authentication")
		connString := config["azure_blob_storage_account_connection_string"]
		if connString == "" {
			return nil, fmt.Errorf("azure blob connection string is empty")
		}

		client, err := azblob.NewClientFromConnectionString(connString, nil)
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
	startTime := time.Now().UTC()
	expiryTime := startTime.Add(tokenExpiryDuration)

	logger.GetLogger().Info(fmt.Sprintf("User Delegation Key expiry duration: %v", tokenExpiryDuration))

	startTimeStr := startTime.Format(time.RFC3339)
	expiryTimeStr := expiryTime.Format(time.RFC3339)

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

	sasExpiryTime := time.Now().UTC().Add(tokenExpiryDuration)
	logger.GetLogger().Info(fmt.Sprintf("SAS token expiry time: %s (duration: %v)", sasExpiryTime.Format(time.RFC3339), tokenExpiryDuration))

	signatureValues := sas.BlobSignatureValues{
		Version:       sas.Version,
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC(),
		ExpiryTime:    sasExpiryTime,
		Permissions:   permissions.String(),
		ContainerName: containerName,
	}

	sasQueryParams, err := signatureValues.SignWithUserDelegation(userDelegationCred)
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to sign SAS with user delegation: %v", err))
		return nil, fmt.Errorf("failed to sign SAS with user delegation: %w", err)
	}

	serviceURLWithSAS := fmt.Sprintf("%s?%s", serviceURL, sasQueryParams.Encode())

	blobClient, err := azblob.NewClientWithNoCredential(serviceURLWithSAS, nil)
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
	serviceClient, err := azblob.NewClient(serviceURL, cred, nil)
	if err != nil {
		logger.GetLogger().Error(fmt.Sprintf("Failed to create Azure Blob service client: %v", err))
		return nil, fmt.Errorf("failed to create Azure Blob service client: %w", err)
	}

	logger.GetLogger().Info("Successfully created Azure Blob client with Service principal")
	return serviceClient, nil
}
