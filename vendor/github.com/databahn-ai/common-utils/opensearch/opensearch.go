package opensearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azsecrets "github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/gcp"
	"github.com/databahn-ai/common-utils/vault"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
)

type Credentials struct {
	OpenSearchUsername            string `json:"open_search.username"`
	OpenSearchPassword            string `json:"open_search.password"`
	OpenSearchUrl                 string `json:"open_search.url"`
	OpenSearchStatisticsIndexName string `json:"open_search.statisticsIndexName"`
	OpenSearchEnableSsl           bool   `json:"open_search.enable_ssl"`
	Client                        *opensearch.Client
}

type Connection struct {
	SecretName string `json:"secret_name"`
	SkipTls    bool   `json:"skip_tls"`
}

// Swapped in tests to avoid external secret backend dependencies.
var readOsDetailsFunc = readOsDetails

// Connect create connection with opensearch
func Connect(config configuration.ConfigReader) (*opensearch.Client, error) {
	credential, err := readOsDetailsFunc(config)
	if err != nil {
		return nil, err
	}

	if !credential.OpenSearchEnableSsl {
		credential.OpenSearchEnableSsl, err = strconv.ParseBool(config.GetString(configuration.OpenSearchSkipTls))
		if err != nil {
			credential.OpenSearchEnableSsl = false
		}
	}

	logger.GetLogger().Info("creating connection with opensearch", zap.String("url", credential.OpenSearchUrl), zap.String("username", credential.OpenSearchUsername))

	return opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: credential.OpenSearchEnableSsl},
		},
		Addresses: strings.Split(credential.OpenSearchUrl, ","),
		Username:  credential.OpenSearchUsername,
		Password:  credential.OpenSearchPassword,
	})
}

// GetConnection creates a connection with OpenSearch.
// Deprecated: use Connect instead.
func GetConnection(config configuration.ConfigReader) (*Credentials, error) {
	var err error
	creds, err := readOsDetailsFunc(config)
	if err != nil {
		logger.GetLogger().Error("error while reading opensearch credentials", zap.Error(err))
		return nil, err
	}

	skipTls, err := strconv.ParseBool(config.GetString(configuration.OpenSearchSkipTls))
	if err != nil {
		skipTls = false
	}
	logger.GetLogger().Info("creating connection with opensearch", zap.String("url", creds.OpenSearchUrl), zap.String("username", creds.OpenSearchUsername))

	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: skipTls},
		},
		Addresses: strings.Split(creds.OpenSearchUrl, ","),
		Username:  creds.OpenSearchUsername,
		Password:  creds.OpenSearchPassword,
	})
	if err != nil {
		logger.GetLogger().Error("error while creating opensearch connection", zap.Error(err))
		return nil, err
	}
	creds.Client = client
	return creds, nil
}

func readOsDetails(config configuration.ConfigReader) (*Credentials, error) {
	secretName := config.GetString(configuration.OpenSearchSecretName)
	if secretName == "" {
		logger.GetLogger().Error("opensearch secret name is empty")
		return nil, errors.New("opensearch secret name is empty")
	}
	secretBackend := config.GetString(configuration.SecretBackend)
	if secretBackend == "" {
		logger.GetLogger().Info("No secret backend configured. selecting default")
		secretBackend = configuration.SecretBackendAWS
	}

	switch secretBackend {
	case configuration.SecretBackendAWS:
		return readOsDetailsFromAWS(config, secretName)
	case configuration.SecretBackendVault:
		return readOsDetailsFromVault(config, secretName)
	case configuration.SecretBackendAzure:
		return readOsDetailsFromAzure(config, secretName)
	case configuration.SecretBackendGCP:
		return readOsDetailsFromGcp(config, secretName)
	default:
		return nil, errors.New("unsupported secret backend: " + secretBackend)
	}
}

func readOsDetailsFromAWS(config configuration.ConfigReader, secretName string) (*Credentials, error) {
	region := config.GetString(configuration.Region)
	data, err := aws.ReadSecretByName(secretName, region)
	if err != nil {
		logger.GetLogger().Error("error while reading secret", zap.Error(err))
		return nil, err
	}
	var creds Credentials
	err = json.Unmarshal([]byte(*data.SecretString), &creds)
	if err != nil {
		return nil, err
	}
	return &creds, nil
}

func readOsDetailsFromVault(config configuration.ConfigReader, secretName string) (*Credentials, error) {
	vaultAddress := config.GetString(configuration.VaultAddress)
	vaultToken := config.GetString(configuration.VaultToken)
	if vaultAddress == "" || vaultToken == "" {
		logger.GetLogger().Error("vault address or token is empty")
		return nil, errors.New("vault address or token is empty")
	}
	data, err := vault.ReadSecrets(vaultAddress, vaultToken, secretName)
	if err != nil {
		logger.GetLogger().Error("error while reading secret from vault", zap.Error(err))
		return nil, err
	}
	return parseOpenSearchCredentialsFromMap(data)
}

func readOsDetailsFromAzure(config configuration.ConfigReader, secretName string) (*Credentials, error) {
	vaultUrl := config.GetString(configuration.AzureInfraKeyVaultUrl)
	if vaultUrl == "" {
		logger.GetLogger().Error("azure key vault url is empty")
		return nil, fmt.Errorf("missing config value %s", configuration.AzureInfraKeyVaultUrl)
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		logger.GetLogger().Error("failed to obtain Azure credential", zap.Error(err))
		return nil, err
	}
	client, err := azsecrets.NewClient(vaultUrl, cred, nil)
	if err != nil {
		logger.GetLogger().Error("failed to create Azure Key Vault client", zap.Error(err))
		return nil, err
	}
	resp, err := client.GetSecret(context.Background(), secretName, "", nil)
	if err != nil {
		logger.GetLogger().Error("failed to get secret from Azure Key Vault", zap.Error(err))
		return nil, err
	}
	if resp.Value == nil {
		return nil, errors.New("secret value is nil in Azure Key Vault response")
	}
	var secretMap map[string]interface{}
	err = json.Unmarshal([]byte(*resp.Value), &secretMap)
	if err != nil {
		logger.GetLogger().Error("failed to unmarshal Azure Key Vault secret value", zap.Error(err))
		return nil, err
	}
	return parseOpenSearchCredentialsFromMap(secretMap)
}

func readOsDetailsFromGcp(config configuration.ConfigReader, secretName string) (*Credentials, error) {
	projectId := config.GetString(configuration.GcpInfraProjectId)
	if projectId == "" {
		logger.GetLogger().Error("gcp infra project id is empty")
		return nil, fmt.Errorf("missing config value %s", configuration.GcpInfraProjectId)
	}
	secretValue, err := gcp.ReadSecretByName(context.Background(), projectId, secretName)
	if err != nil {
		logger.GetLogger().Error("failed to get secret from GCP Secret Manager", zap.Error(err))
		return nil, err
	}
	var secretMap map[string]interface{}
	err = json.Unmarshal([]byte(secretValue), &secretMap)
	if err != nil {
		logger.GetLogger().Error("failed to unmarshal GCP Secret Manager secret value", zap.Error(err))
		return nil, err
	}
	return parseOpenSearchCredentialsFromMap(secretMap)
}

func parseOpenSearchCredentialsFromMap(data map[string]interface{}) (*Credentials, error) {
	var creds Credentials
	if username, ok := data["open_search.username"].(string); ok {
		creds.OpenSearchUsername = username
	} else if username, ok := data["opensearch_username"].(string); ok {
		creds.OpenSearchUsername = username
	} else {
		return nil, errors.New("username is missing or not a string in secret data")
	}

	if password, ok := data["open_search.password"].(string); ok {
		creds.OpenSearchPassword = password
	} else if password, ok := data["opensearch_password"].(string); ok {
		creds.OpenSearchPassword = password
	} else {
		return nil, errors.New("password is missing or not a string in secret data")
	}

	if osUrl, ok := data["open_search.url"].(string); ok {
		creds.OpenSearchUrl = osUrl
	} else if osUrl, ok := data["opensearch_url"].(string); ok {
		creds.OpenSearchUrl = osUrl
	} else {
		return nil, errors.New("url is missing or not a string in secret data")
	}

	if index, ok := data["open_search.statisticsIndexName"].(string); ok {
		creds.OpenSearchStatisticsIndexName = index
	} else if index, ok := data["opensearch_statisticsIndexName"].(string); ok {
		creds.OpenSearchStatisticsIndexName = index
	} else {
		return nil, errors.New("statistics index name is missing or not a string in secret data")
	}

	if v, ok := data["open_search.enable_ssl"].(bool); ok {
		creds.OpenSearchEnableSsl = v
	} else if v, ok := data["opensearch_enable_ssl"].(bool); ok {
		creds.OpenSearchEnableSsl = v
	} else if v, ok := data["open_search.enable_ssl"].(string); ok {
		b, _ := strconv.ParseBool(v)
		creds.OpenSearchEnableSsl = b
	} else if v, ok := data["opensearch_enable_ssl"].(string); ok {
		b, _ := strconv.ParseBool(v)
		creds.OpenSearchEnableSsl = b
	} // else leave as default false
	return &creds, nil
}
