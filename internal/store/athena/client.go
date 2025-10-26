package athena

import (
	"context"
	"fmt"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	//"github.com/databahn-ai/common-utils/configuration"
	//appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var (
	once    sync.Once
	client  *athena.Client
	initErr error
)

// GetClient returns a singleton Athena client
func GetClient(ctx context.Context) (*athena.Client, error) {
	once.Do(func() {
		region := appConfig.GetAppConfiguration().GetString("region")
		if region == "" {
			initErr = fmt.Errorf("AWS region not configured")
			logger.GetLogger().Error("AWS region not configured")
			return
		}

		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			initErr = fmt.Errorf("failed to load AWS config: %w", err)
			logger.GetLogger().Error("failed to load AWS config", zap.Error(err))
			return
		}

		client = athena.NewFromConfig(cfg)
		//logger.GetLogger().Info("Athena client initialized")
		logger.GetLogger().Info("Athena client initialized", zap.String("region", region))
	})

	if initErr != nil {
		return nil, initErr
	}

	return client, nil
}
