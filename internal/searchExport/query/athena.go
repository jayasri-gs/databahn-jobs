package query

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type AthenaConfig struct {
	Region          string
	Workgroup       string
	OutputLocation  string
	QueryTimeout    time.Duration
	AuthType        string
	AccessKeyID     string
	SecretAccessKey string
	RoleArn         string
	ExternalID      string
}

type AthenaExecutor struct {
	cfg       AthenaConfig
	client    *athena.Client
	awsConfig aws.Config
	log       *zap.Logger
}

func NewAthenaExecutor(cfg AthenaConfig) *AthenaExecutor {
	if cfg.QueryTimeout == 0 {
		cfg.QueryTimeout = 30 * time.Minute
	}
	return &AthenaExecutor{cfg: cfg, log: logging.GetLogger()}
}

func (e *AthenaExecutor) SetLogger(log *zap.Logger) {
	if log != nil {
		e.log = log
	}
}

func (e *AthenaExecutor) Connect(ctx context.Context) error {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(e.cfg.Region))

	if e.cfg.AuthType == "role_based" {
		baseCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(e.cfg.Region))
		if err != nil {
			return fmt.Errorf("failed to load base AWS config: %w", err)
		}
		stsClient := sts.NewFromConfig(baseCfg)
		opts = append(opts, awsconfig.WithCredentialsProvider(
			stscreds.NewAssumeRoleProvider(stsClient, e.cfg.RoleArn, func(o *stscreds.AssumeRoleOptions) {
				if e.cfg.ExternalID != "" {
					o.ExternalID = aws.String(e.cfg.ExternalID)
				}
			}),
		))
	} else if e.cfg.AccessKeyID != "" && e.cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				e.cfg.AccessKeyID,
				e.cfg.SecretAccessKey,
				"",
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	e.awsConfig = awsCfg
	e.client = athena.NewFromConfig(awsCfg)
	e.log.Info("Connected to Athena",
		zap.String("region", e.cfg.Region),
		zap.String("workgroup", e.cfg.Workgroup),
		zap.String("outputLocation", e.cfg.OutputLocation),
		zap.String("authType", e.cfg.AuthType))
	return nil
}

func (e *AthenaExecutor) ExecuteUnload(ctx context.Context, query, database, s3OutputPath string) (*UnloadResult, error) {
	unloadQuery := fmt.Sprintf(
		"UNLOAD (%s) TO '%s' WITH (format = 'PARQUET', compression = 'SNAPPY')",
		query, s3OutputPath,
	)

	e.log.Info("Executing UNLOAD query",
		zap.String("database", database),
		zap.String("outputPath", s3OutputPath),
		zap.String("sqlQuery", query))

	startInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(unloadQuery),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Database: aws.String(database),
		},
		WorkGroup: aws.String(e.cfg.Workgroup),
	}

	if e.cfg.OutputLocation != "" {
		startInput.ResultConfiguration = &athenatypes.ResultConfiguration{
			OutputLocation: aws.String(e.cfg.OutputLocation),
		}
	}

	startOutput, err := e.client.StartQueryExecution(ctx, startInput)
	if err != nil {
		return nil, fmt.Errorf("failed to start UNLOAD query: %w", err)
	}

	queryID := *startOutput.QueryExecutionId
	e.log.Info("Started UNLOAD query", zap.String("athenaQueryExecutionId", queryID))

	if err := e.waitForCompletion(ctx, queryID); err != nil {
		return nil, err
	}

	execOutput, err := e.client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(queryID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get query execution details: %w", err)
	}

	result := &UnloadResult{
		OutputLocation: s3OutputPath,
	}

	if execOutput.QueryExecution != nil && execOutput.QueryExecution.Statistics != nil {
		stats := execOutput.QueryExecution.Statistics
		if stats.DataManifestLocation != nil {
			result.ManifestLocation = *stats.DataManifestLocation
		}
		if stats.DataScannedInBytes != nil {
			result.BytesScanned = *stats.DataScannedInBytes
		}
	}

	e.log.Info("UNLOAD query completed",
		zap.String("athenaQueryExecutionId", queryID),
		zap.Int64("bytesScanned", result.BytesScanned),
		zap.String("manifestLocation", result.ManifestLocation))

	return result, nil
}

func (e *AthenaExecutor) waitForCompletion(ctx context.Context, queryID string) error {
	deadline := time.Now().Add(e.cfg.QueryTimeout)

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("query timed out after %v", e.cfg.QueryTimeout)
		}

		output, err := e.client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return fmt.Errorf("failed to get query status: %w", err)
		}

		state := output.QueryExecution.Status.State
		e.log.Debug("Polling Athena query status",
			zap.String("athenaQueryExecutionId", queryID),
			zap.String("state", string(state)))

		switch state {
		case athenatypes.QueryExecutionStateSucceeded:
			e.log.Info("Athena query succeeded", zap.String("athenaQueryExecutionId", queryID))
			return nil
		case athenatypes.QueryExecutionStateFailed:
			reason := ""
			if output.QueryExecution.Status.StateChangeReason != nil {
				reason = *output.QueryExecution.Status.StateChangeReason
			}
			return fmt.Errorf("query failed: %s", reason)
		case athenatypes.QueryExecutionStateCancelled:
			return fmt.Errorf("query was cancelled")
		}

		time.Sleep(2 * time.Second)
	}
}

func (e *AthenaExecutor) GetAWSConfig() interface{} {
	return e.awsConfig
}

func (e *AthenaExecutor) GetOutputLocation() string {
	return e.cfg.OutputLocation
}

func (e *AthenaExecutor) Close() error {
	return nil
}
