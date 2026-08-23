package authentication

import (
	"context"
	"fmt"
	"github.com/databahn-ai/common-utils/configuration"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
)

var oauthConfig *clientcredentials.Config

var tokenUrlTemplate = "https://%s/realms/%s/protocol/openid-connect/token"

func buildAuthClient(ctx context.Context, appConfig configuration.ConfigReader) (*http.Client, error) {
	if oauthConfig == nil {
		creds, err := configuration.ReadOAuthClientCredentials(appConfig)
		if err != nil {
			return nil, err
		}
		authBaseUrl := appConfig.GetString(configuration.AuthenticationUrl)
		tokenUrl := fmt.Sprintf(tokenUrlTemplate, authBaseUrl, Realm)
		oauthConfig = &clientcredentials.Config{
			ClientID:     creds.ServiceAccountClient,
			ClientSecret: creds.ServiceAccountSecret,
			TokenURL:     tokenUrl,
			Scopes:       []string{ScopeOpenId},
		}
	}
	return oauthConfig.Client(ctx), nil
}

func GetOAuthHttpClient(ctx context.Context, reader configuration.ConfigReader) (*http.Client, error) {
	return buildAuthClient(ctx, reader)
}
