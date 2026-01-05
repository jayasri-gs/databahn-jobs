package opensearch

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/configuration"
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

// Connect create connection with opensearch
func Connect(config configuration.ConfigReader) (*opensearch.Client, error) {
	var err error
	creds, err := readOsDetails(config)
	if err != nil {
		logger.GetLogger().Error("error while reading opensearch credentials", zap.Error(err))
		return nil, err
	}

	skipTls, err := strconv.ParseBool(config.GetString(configuration.OpenSearchSkipTls))
	if err != nil {
		skipTls = false
	}
	logger.GetLogger().Info("creating connection with opensearch", zap.String("url", creds.OpenSearchUrl), zap.String("username", creds.OpenSearchUsername))

	return opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: skipTls},
		},
		Addresses: strings.Split(creds.OpenSearchUrl, ","),
		Username:  creds.OpenSearchUsername,
		Password:  creds.OpenSearchPassword,
	})
}

// GetConnection create connection with opensearch
func GetConnection(config configuration.ConfigReader) (*Credentials, error) {
	var err error
	creds, err := readOsDetails(config)
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
	var creds Credentials
	switch secretBackend {
	case configuration.SecretBackendAWS:
		region := config.GetString(configuration.Region)
		data, err := aws.ReadSecretByName(secretName, region)
		if err != nil {
			logger.GetLogger().Error("error while reading secret", zap.Error(err))
			return nil, err
		}
		err = json.Unmarshal([]byte(*data.SecretString), &creds)
		if err != nil {
			return nil, err
		}
	case configuration.SecretBackendVault:
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
		if username, ok := data["open_search.username"].(string); ok {
			creds.OpenSearchUsername = username
		} else {
			return nil, errors.New("username is missing or not a string in secret data")
		}

		if password, ok := data["open_search.password"].(string); ok {
			creds.OpenSearchPassword = password
		} else {
			return nil, errors.New("password is missing or not a string in secret data")
		}

		if osUrl, ok := data["open_search.url"].(string); ok {
			creds.OpenSearchUrl = osUrl
		} else {
			return nil, errors.New("url is missing or not a string in secret data")
		}

		if index, ok := data["open_search.statisticsIndexName"].(string); ok {
			creds.OpenSearchStatisticsIndexName = index
		} else {
			return nil, errors.New("statistics index name is missing or not a string in secret data")
		}
	}
	return &creds, nil
}
