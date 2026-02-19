package athena

import (
	"context"
	"fmt"
	"sync"

	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"

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

// RunDDLInRegion executes a DDL query in a specific AWS region with a custom output location.
func RunDDLInRegion(ctx context.Context, query, region, outputLocation string) error {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("failed to load AWS config for region %s: %w", region, err)
	}
	c := athena.NewFromConfig(cfg)

	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		},
	}
	result, err := c.StartQueryExecution(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	return waitForQueryCompletion(ctx, c, *result.QueryExecutionId)
}

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
