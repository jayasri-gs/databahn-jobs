package configuration

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/common-utils/aws"
)

type OpenSearchCredentials struct {
	Url                 string `json:"url"`
	Username            string `json:"username"`
	Password            string `json:"password"`
	StatsIndexName      string `json:"statsIndexName"`
	StatisticsIndexName string `json:"statisticsIndexName"`
}

func ReadOpenSearchSecrets(ctx context.Context, secretName, region string) (*OpenSearchCredentials, error) {
	data, err := aws.ReadSecretByName(secretName, region)
	if err != nil {
		return nil, err
	}

	creds := &OpenSearchCredentials{}
	err = json.Unmarshal([]byte(*data.SecretString), &creds)
	if err != nil {
		return nil, err
	}
	return creds, nil
}

type OAuthClientCredentials struct {
	ServiceAccountClient string `json:"service_account_client"`
	ServiceAccountSecret string `json:"service_account_secret"`
}

func ReadOAuthClientCredentials(secretName, region string) (*OAuthClientCredentials, error) {
	data, err := aws.ReadSecretByName(secretName, region)
	if err != nil {
		return nil, err
	}

	creds := &OAuthClientCredentials{}
	err = json.Unmarshal([]byte(*data.SecretString), &creds)
	if err != nil {
		return nil, err
	}
	return creds, nil
}
