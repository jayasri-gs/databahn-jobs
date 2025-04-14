package opensearch

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/databahn-ai/common-utils/aws"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/vault"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"net/http"
	"strconv"
	"strings"
)

type Credentials struct {
	Username string `json:"open_search.username"`
	Password string `json:"open_search.password"`
}

type Connection struct {
	Url        string `json:"opensearch_url"`
	SecretName string `json:"secret_name"`
	SkipTls    bool   `json:"skip_tls"`
}

// Connect create connection with opensearch
func Connect(config configuration.ConfigReader) (*opensearch.Client, error) {
	var err error
	url := config.GetString(configuration.OpenSearchUrl)
	if url == "" {
		logger.GetLogger().Error("opensearch url is empty")
		return nil, errors.New("opensearch url is empty")
	}
	if !strings.HasPrefix(url, "http") {
		url = "https://" + url
	}

	creds, err := readCredentials(config)
	if err != nil {
		logger.GetLogger().Error("error while reading opensearch credentials", zap.Error(err))
		return nil, err
	}

	skipTls, err := strconv.ParseBool(config.GetString(configuration.OpenSearchSkipTls))
	if err != nil {
		skipTls = false
	}
	logger.GetLogger().Info("creating connection with opensearch", zap.String("url", url))

	return opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: skipTls},
		},
		Addresses: strings.Split(url, ","),
		Username:  creds.Username,
		Password:  creds.Password,
	})
}

func readCredentials(config configuration.ConfigReader) (*Credentials, error) {
	secretName := config.GetString(configuration.OpenSearchSecretName)
	if secretName == "" {
		logger.GetLogger().Error("opensearch secret name is empty")
		return nil, errors.New("opensearch secret name is empty")
	}
	secretBackend := config.GetString(configuration.SecretBackend)
	if secretBackend == "" {
		logger.GetLogger().Error("opensearch secret backend is empty")
		return nil, errors.New("opensearch secret backend is empty")
	}
	var creds Credentials
	switch secretBackend {
	case "aws":
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
	case "vault":
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
			creds.Username = username
		} else {
			return nil, errors.New("username is missing or not a string in secret data")
		}

		if password, ok := data["open_search.password"].(string); ok {
			creds.Password = password
		} else {
			return nil, errors.New("password is missing or not a string in secret data")
		}
	}
	return &creds, nil
}
