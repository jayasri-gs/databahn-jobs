package os

import (
	"context"
	"crypto/tls"
	"net/http"

	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
)

type Creds struct {
	Username string
	Password string
}

func NewClient(ctx context.Context, host string, creds *Creds) (*opensearch.Client, error) {
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS12,
			},
		},
		Addresses: []string{host},
		Username:  creds.Username,
		Password:  creds.Password,
	})
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Failed to create Open search client", zap.Error(err))
	}
	return client, err
}
