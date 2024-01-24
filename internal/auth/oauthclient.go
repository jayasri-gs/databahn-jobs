package auth

import (
	"context"
	"fmt"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
)

var client *http.Client
var tokenUrlTemplate = "https://%s/realms/%s/protocol/openid-connect/token"

func buildAuthClient(ctx context.Context) (*http.Client, error) {
	secretName := config.GetAppConfiguration().GetString(configuration.OAuthClientCredentialsSecretName)
	creds, err := configuration.ReadOAuthClientCredentials(secretName)
	if err != nil {
		return nil, err
	}
	authBaseUrl := config.GetAppConfiguration().GetString(configuration.AuthenticationUrl)
	tokenUrl := fmt.Sprintf(tokenUrlTemplate, authBaseUrl, Realm)
	config := clientcredentials.Config{
		ClientID:     creds.ClientId,
		ClientSecret: creds.ClientSecret,
		TokenURL:     tokenUrl,
		Scopes:       []string{ScopeOpenId},
	}
	return config.Client(ctx), nil
}

func GetOAuthHttpClient(ctx context.Context) (*http.Client, error) {
	if client == nil {
		cli, err := buildAuthClient(ctx)
		if err != nil {
			return nil, err
		}
		client = cli
	}
	return client, nil
}
