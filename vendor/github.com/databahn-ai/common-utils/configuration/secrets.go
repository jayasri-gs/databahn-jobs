package configuration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azsecrets "github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/gcp"
	"github.com/databahn-ai/common-utils/vault"
)

type OpenSearchCredentials struct {
	Url                 string `json:"url"`
	Username            string `json:"username"`
	Password            string `json:"password"`
	StatsIndexName      string `json:"statsIndexName"`
	StatisticsIndexName string `json:"statisticsIndexName"`
}

func ReadOpenSearchSecrets(ctx context.Context, config ConfigReader) (*OpenSearchCredentials, error) {
	secretName := config.GetString(OpenSearchSecretName)
	if secretName == "" {
		return nil, fmt.Errorf("opensearch secret name not found in config")
	}
	secretValue, err := readInfraSecret(ctx, config, secretName)
	if err != nil {
		return nil, err
	}
	creds := &OpenSearchCredentials{}
	err = json.Unmarshal([]byte(secretValue), creds)
	if err != nil {
		return nil, err
	}
	return creds, nil
}

type OAuthClientCredentials struct {
	ServiceAccountClient string `json:"service_account_client"`
	ServiceAccountSecret string `json:"service_account_secret"`
}

func ReadOAuthClientCredentials(config ConfigReader) (*OAuthClientCredentials, error) {
	secretName := config.GetString(OAuthClientCredentialsSecretName)
	if secretName == "" {
		return nil, fmt.Errorf("oauth client credentials secret name not found in config")
	}
	secretValue, err := readInfraSecret(context.Background(), config, secretName)
	if err != nil {
		return nil, err
	}
	creds := &OAuthClientCredentials{}
	err = json.Unmarshal([]byte(secretValue), creds)
	if err != nil {
		return nil, err
	}
	return creds, nil
}

func readInfraSecret(ctx context.Context, config ConfigReader, secretName string) (string, error) {
	secretBackend := config.GetString(SecretBackend)
	if secretBackend == "" {
		secretBackend = SecretBackendAWS
	}

	switch secretBackend {
	case SecretBackendAWS:
		data, err := aws.ReadSecretByName(secretName, config.GetString(Region))
		if err != nil {
			return "", err
		}
		return *data.SecretString, nil
	case SecretBackendVault:
		secretName = strings.TrimPrefix(secretName, "vault://")
		vaultAddress := config.GetString(VaultAddress)
		vaultToken := config.GetString(VaultToken)
		if vaultAddress == "" || vaultToken == "" {
			return "", fmt.Errorf("vault address or token not found in config")
		}
		secretData, err := vault.ReadSecrets(vaultAddress, vaultToken, secretName)
		if err != nil {
			return "", err
		}
		secretBytes, err := json.Marshal(secretData)
		if err != nil {
			return "", err
		}
		return string(secretBytes), nil
	case SecretBackendAzure:
		vaultUrl := config.GetString(AzureInfraKeyVaultUrl)
		if vaultUrl == "" {
			return "", fmt.Errorf("missing config value %s", AzureInfraKeyVaultUrl)
		}
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return "", fmt.Errorf("failed to obtain Azure credential: %w", err)
		}
		client, err := azsecrets.NewClient(vaultUrl, cred, nil)
		if err != nil {
			return "", fmt.Errorf("failed to create Azure Key Vault client: %w", err)
		}
		resp, err := client.GetSecret(ctx, secretName, "", nil)
		if err != nil {
			return "", fmt.Errorf("failed to get secret from Azure Key Vault: %w", err)
		}
		if resp.Value == nil {
			return "", fmt.Errorf("secret value is nil in Azure Key Vault response")
		}
		return *resp.Value, nil
	case SecretBackendGCP:
		projectId := config.GetString(GcpInfraProjectId)
		if projectId == "" {
			return "", fmt.Errorf("missing config value %s", GcpInfraProjectId)
		}
		return gcp.ReadSecretByName(ctx, projectId, secretName)
	default:
		return "", fmt.Errorf("invalid secret backend: %s", secretBackend)
	}
}
