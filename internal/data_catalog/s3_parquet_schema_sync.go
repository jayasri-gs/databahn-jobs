package data_catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"

	changeflag "github.com/databahn-ai/db-models/changeflag"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type destinationRow struct {
	ID            uuid.UUID `gorm:"column:id"`
	TenantID      uuid.UUID `gorm:"column:tenant_id"`
	Configuration string    `gorm:"column:configuration"`
	SecretID      *string   `gorm:"column:secret_id"`
}

type destinationConfig struct {
	AuthType    string `json:"auth_type"`
	AccessKeyID string `json:"access_key_id"`
	SecretKey   string `json:"secret_access_key"`
	RoleArn     string `json:"role_arn"`
	ExternalID  string `json:"external_id"`
	Region      string `json:"region"`
	Bucket      string `json:"bucket"`
}

func ApplyS3ParquetCatalogToAthena(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	db := appConfig.GetDB()

	var fields []catalogField
	err := db.WithContext(ctx).
		Where("applied_on_search = ? AND dispenser_type = ?", false, "s3parquet").
		Find(&fields).Error
	if err != nil {
		return common.NewJobResultFromError(fmt.Errorf("failed to query data_catalog for s3parquet: %w", err))
	}

	if len(fields) == 0 {
		logger.GetLoggerWithContext(ctx).Info("no unapplied s3 parquet data catalog fields found")
		return common.NewJobResultSuccess()
	}

	logger.GetLoggerWithContext(ctx).Info("found unapplied s3 parquet data catalog fields", zap.Int("count", len(fields)))

	groups := make(map[string][]catalogField)
	for _, f := range fields {
		key := groupKey(f.DestID, f.SourceID, f.TenantID)
		groups[key] = append(groups[key], f)
	}

	for _, group := range groups {
		first := group[0]
		err := processS3ParquetGroup(ctx, first.DestID, first.SourceID, first.TenantID, group)
		if err != nil {
			if isTableNotFound(err) {
				logger.GetLoggerWithContext(ctx).Warn("skipping s3 parquet group — Athena table or config not found, will retry on next run",
					zap.String("destination_id", first.DestID.String()),
					zap.String("source_id", first.SourceID.String()),
					zap.Error(err))
				continue
			}

			logger.GetLoggerWithContext(ctx).Error("failed to apply s3 parquet catalog fields to Athena",
				zap.String("destination_id", first.DestID.String()),
				zap.String("source_id", first.SourceID.String()),
				zap.Error(err))
			jobErrors = append(jobErrors, common.JobError{Message: err.Error()})
			continue
		}

		ids := make([]int64, len(group))
		for i, f := range group {
			ids[i] = f.ID
		}
		if err := appConfig.GetDB().WithContext(ctx).
			Table("data_catalog").
			Where("id IN ?", ids).
			Updates(map[string]interface{}{
				"applied_on_search": true,
				"search_engine":     "ATHENA",
			}).Error; err != nil {
			logger.GetLoggerWithContext(ctx).Error("failed to mark s3 parquet catalog fields as applied", zap.Error(err))
			jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to update applied_on_search: %v", err)})
		}
	}

	if len(jobErrors) > 0 {
		return common.NewJobResultFromErrors(jobErrors)
	}
	return common.NewJobResultSuccess()
}

func processS3ParquetGroup(ctx context.Context, destID, sourceID, tenantID uuid.UUID, fields []catalogField) error {
	db := appConfig.GetDB()

	// Look up data store for this destination
	var storeIDStr string
	err := db.WithContext(ctx).Raw(
		"SELECT id FROM search_data_store WHERE destination_id = ? AND type = 'DATABAHN_DESTINATION' LIMIT 1",
		destID,
	).Scan(&storeIDStr).Error
	if err != nil || storeIDStr == "" {
		return newTableNotFoundError("search_data_store not found for s3 parquet destination %s", destID)
	}

	// Look up search_configuration
	var searchConfigJSON string
	err = db.WithContext(ctx).Raw(
		"SELECT search_configuration FROM search_data_set WHERE store_id = ? AND source_id = ? LIMIT 1",
		storeIDStr, sourceID,
	).Scan(&searchConfigJSON).Error
	if err != nil || searchConfigJSON == "" {
		return newTableNotFoundError("search_data_set not found for s3 parquet store %s, source %s", storeIDStr, sourceID)
	}

	var sc searchConfig
	if err := json.Unmarshal([]byte(searchConfigJSON), &sc); err != nil {
		return fmt.Errorf("failed to parse search_configuration: %w", err)
	}
	tableName := sc.S3Configuration.AthenaTable
	if tableName == "" {
		return newTableNotFoundError("athenaTable is empty for s3 parquet store %s, source %s", storeIDStr, sourceID)
	}

	// Get destination config with secrets for customer Athena access
	destConfig, err := getDestinationConfig(ctx, destID, tenantID)
	if err != nil {
		return fmt.Errorf("failed to get destination config for %s: %w", destID, err)
	}

	if destConfig.Region == "" {
		return fmt.Errorf("region is empty in destination config for %s", destID)
	}

	outputLocation := fmt.Sprintf("s3://%s/.databahn_out/", destConfig.Bucket)
	database := databaseName(tenantID)

	var colDefs []string
	for _, f := range fields {
		colDefs = append(colDefs, fmt.Sprintf("%s %s", f.Name, athenaType(f.FieldType)))
	}
	query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMNS (%s)", database, tableName, strings.Join(colDefs, ", "))

	logger.GetLoggerWithContext(ctx).Info("executing s3 parquet Athena ALTER TABLE",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.String("region", destConfig.Region),
		zap.Int("columns", len(colDefs)))

	athenaClient, err := createCustomerAthenaClient(ctx, destConfig)
	if err != nil {
		return fmt.Errorf("failed to create customer Athena client for %s: %w", destID, err)
	}

	if err := runDDLWithClient(ctx, athenaClient, query, outputLocation); err != nil {
		return fmt.Errorf("ALTER TABLE failed for %s.%s: %w", database, tableName, err)
	}

	logger.GetLoggerWithContext(ctx).Info("successfully applied s3 parquet catalog fields to customer Athena",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.Int("fields_added", len(colDefs)))

	return nil
}

func getDestinationConfig(ctx context.Context, destID, tenantID uuid.UUID) (*destinationConfig, error) {
	db := appConfig.GetDB()

	var dest destinationRow
	err := db.WithContext(ctx).Raw(
		"SELECT id, tenant_id, configuration, secret_id FROM destination WHERE id = ? LIMIT 1",
		destID,
	).Scan(&dest).Error
	if err != nil {
		return nil, fmt.Errorf("destination not found: %w", err)
	}

	var cfg destinationConfig
	if dest.Configuration != "" {
		if err := json.Unmarshal([]byte(dest.Configuration), &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse destination configuration: %w", err)
		}
	}

	if dest.SecretID != nil && *dest.SecretID != "" {
		secretIdsByTenant := map[string][]string{
			tenantID.String(): {*dest.SecretID},
		}
		secrets, err := changeflag.LoadSecrets(appConfig.GetAppConfiguration(), secretIdsByTenant)
		if err != nil {
			return nil, fmt.Errorf("failed to load secrets: %w", err)
		}
		for _, secret := range secrets {
			if secretMap, ok := secret.Secrets[*dest.SecretID]; ok {
				for k, v := range secretMap {
					switch k {
					case "access_key_id":
						cfg.AccessKeyID = v
					case "secret_access_key":
						cfg.SecretKey = v
					case "role_arn":
						cfg.RoleArn = v
					case "external_id":
						cfg.ExternalID = v
					}
				}
			}
			if errMsg, ok := secret.Errors[*dest.SecretID]; ok {
				return nil, fmt.Errorf("secret resolution failed: %s", errMsg)
			}
		}
	}

	return &cfg, nil
}

func createCustomerAthenaClient(ctx context.Context, cfg *destinationConfig) (*athena.Client, error) {
	var credentialsProvider aws.CredentialsProvider

	if cfg.AuthType == "role_based" {
		baseCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
		if err != nil {
			return nil, fmt.Errorf("failed to load base AWS config: %w", err)
		}
		stsClient := sts.NewFromConfig(baseCfg)
		credentialsProvider = stscreds.NewAssumeRoleProvider(stsClient, cfg.RoleArn, func(o *stscreds.AssumeRoleOptions) {
			if cfg.ExternalID != "" {
				o.ExternalID = aws.String(cfg.ExternalID)
			}
		})
	} else {
		credentialsProvider = credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretKey, "")
	}

	awsCfg := aws.Config{
		Region:      cfg.Region,
		Credentials: aws.NewCredentialsCache(credentialsProvider),
	}
	return athena.NewFromConfig(awsCfg), nil
}

func runDDLWithClient(ctx context.Context, client *athena.Client, query, outputLocation string) error {
	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		},
	}
	result, err := client.StartQueryExecution(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	executionID := *result.QueryExecutionId
	for {
		status, err := client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(executionID),
		})
		if err != nil {
			return fmt.Errorf("failed to get query status: %w", err)
		}
		state := status.QueryExecution.Status.State
		switch state {
		case types.QueryExecutionStateSucceeded:
			return nil
		case types.QueryExecutionStateFailed:
			reason := ""
			if status.QueryExecution.Status.StateChangeReason != nil {
				reason = *status.QueryExecution.Status.StateChangeReason
			}
			return fmt.Errorf("query failed: %s", reason)
		case types.QueryExecutionStateCancelled:
			return fmt.Errorf("query was cancelled")
		}
	}
}
