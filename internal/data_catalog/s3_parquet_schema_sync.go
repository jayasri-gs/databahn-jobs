package data_catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"

	dbaws "github.com/databahn-ai/common-utils/aws"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type destinationRow struct {
	ID            uuid.UUID `gorm:"column:id"`
	TenantID      uuid.UUID `gorm:"column:tenant_id"`
	Configuration string    `gorm:"column:configuration"`
}

type destinationConfigWrapper struct {
	SecretID      string                 `json:"secretId"`
	Configuration map[string]interface{} `json:"configuration"`
}

func (w *destinationConfigWrapper) getString(key string) string {
	if v, ok := w.Configuration[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
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
		Where("applied_on_search = ? AND dispenser_type = ?", false, "S3Parquet").
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

var validSQLIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

func quoteSQLIdentifier(name string) (string, error) {
	if !validSQLIdentifier.MatchString(name) {
		return "", fmt.Errorf("invalid SQL identifier: %q", name)
	}
	return "`" + name + "`", nil
}

func processS3ParquetGroup(ctx context.Context, destID, sourceID, tenantID uuid.UUID, fields []catalogField) error {
	db := appConfig.GetDB()

	// Look up data-store for this destination
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

	quotedDB, err := quoteSQLIdentifier(database)
	if err != nil {
		return fmt.Errorf("invalid database name: %w", err)
	}
	quotedTable, err := quoteSQLIdentifier(tableName)
	if err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// Validate fields: drop leading-underscore, reserved-keyword, and duplicate
	// names, comparing duplicates against already-applied columns for this table.
	// Only fetch applied names that could actually collide with an incoming field.
	incomingLower := lowerFieldNames(fields)
	var existingApplied []string
	if err := db.WithContext(ctx).
		Table("data_catalog").
		Where("applied_on_search = ? AND destination_id = ? AND source_id = ? AND tenant_id = ? AND dispenser_type = ? AND LOWER(name) IN ?",
			true, destID, sourceID, tenantID, "S3Parquet", incomingLower).
		Pluck("name", &existingApplied).Error; err != nil {
		return fmt.Errorf("failed to load applied catalog fields for %s/%s: %w", destID, sourceID, err)
	}

	valid, invalid := partitionCatalogFields(fields, existingApplied)

	if len(invalid) > 0 {
		invalidIDs := make([]int64, len(invalid))
		for i, inv := range invalid {
			invalidIDs[i] = inv.field.ID
			logger.GetLoggerWithContext(ctx).Warn("deleting invalid catalog field",
				zap.String("name", inv.field.Name),
				zap.String("reason", inv.reason),
				zap.Int64("id", inv.field.ID))
		}
		if err := db.WithContext(ctx).
			Table("data_catalog").
			Where("id IN ?", invalidIDs).
			Delete(nil).Error; err != nil {
			return fmt.Errorf("failed to delete invalid catalog fields: %w", err)
		}
	}

	if len(valid) == 0 {
		logger.GetLoggerWithContext(ctx).Info("no valid s3 parquet catalog fields to apply after validation",
			zap.String("destination_id", destID.String()),
			zap.String("source_id", sourceID.String()))
		return nil
	}

	var colDefs []string
	for _, f := range valid {
		quotedCol, err := quoteSQLIdentifier(f.Name)
		if err != nil {
			return fmt.Errorf("invalid column name %q: %w", f.Name, err)
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s", quotedCol, athenaType(f.FieldType)))
	}
	query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMNS (%s)", quotedDB, quotedTable, strings.Join(colDefs, ", "))

	logger.GetLoggerWithContext(ctx).Info("executing s3 parquet Athena ALTER TABLE",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.String("region", destConfig.Region),
		zap.String("outputLocation", outputLocation),
		zap.Int("columns", len(colDefs)),
		zap.String("query", query))

	athenaClient, err := createCustomerAthenaClient(ctx, destConfig)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("failed to create customer Athena client",
			zap.String("destination_id", destID.String()),
			zap.String("auth_type", destConfig.AuthType),
			zap.String("region", destConfig.Region),
			zap.Error(err))
		return fmt.Errorf("failed to create customer Athena client for %s: %w", destID, err)
	}
	logger.GetLoggerWithContext(ctx).Info("customer Athena client created",
		zap.String("destination_id", destID.String()),
		zap.String("auth_type", destConfig.AuthType),
		zap.String("region", destConfig.Region))

	if err := runDDLWithClient(ctx, athenaClient, query, outputLocation); err != nil {
		logger.GetLoggerWithContext(ctx).Error("ALTER TABLE failed",
			zap.String("database", database),
			zap.String("table", tableName),
			zap.String("query", query),
			zap.Error(err))
		return fmt.Errorf("ALTER TABLE failed for %s.%s: %w", database, tableName, err)
	}

	logger.GetLoggerWithContext(ctx).Info("ALTER TABLE succeeded — s3 parquet catalog fields applied",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.String("destination_id", destID.String()),
		zap.Int("fields_added", len(colDefs)))

	return nil
}

func getDestinationConfig(ctx context.Context, destID, tenantID uuid.UUID) (*destinationConfig, error) {
	db := appConfig.GetDB()

	var dest destinationRow
	err := db.WithContext(ctx).Raw(
		"SELECT id, tenant_id, configuration FROM destination WHERE id = ? AND tenant_id = ? LIMIT 1",
		destID, tenantID,
	).Scan(&dest).Error
	if err != nil {
		return nil, fmt.Errorf("destination not found: %w", err)
	}

	if dest.Configuration == "" {
		return nil, fmt.Errorf("destination configuration is empty for %s", destID)
	}

	var wrapper destinationConfigWrapper
	if err := json.Unmarshal([]byte(dest.Configuration), &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse destination configuration: %w", err)
	}

	cfg := &destinationConfig{
		AuthType:    wrapper.getString("auth_type"),
		AccessKeyID: wrapper.getString("access_key_id"),
		SecretKey:   wrapper.getString("secret_access_key"),
		RoleArn:     wrapper.getString("role_arn"),
		ExternalID:  wrapper.getString("external_id"),
		Region:      wrapper.getString("region"),
		Bucket:      wrapper.getString("bucket"),
	}

	// Fetch credentials from AWS Secrets Manager if a secret reference is configured.
	// SECURITY: No sensitive values are logged - error messages contain only UUIDs and bucket names.
	// The backendSecretID is a reference name (e.g. "prod/app/creds"), not the secret value itself.
	// nolint:gosec // CWE-532 false positive: taint analysis flags variable names, but no secrets are logged
	if wrapper.SecretID != "" {
		var backendSecretID string
		err := db.WithContext(ctx).Raw(
			"SELECT backend_secret_id FROM secrets WHERE id = ? LIMIT 1",
			wrapper.SecretID,
		).Scan(&backendSecretID).Error
		if err != nil || backendSecretID == "" {
			return nil, fmt.Errorf("failed to look up backend_secret_id: secret reference not found or inaccessible (destination=%s, tenant=%s, bucket=%s)", destID, dest.TenantID, cfg.Bucket)
		}

		secretOutput, err := dbaws.ReadSecretByName(backendSecretID, appConfig.GetAppConfiguration().GetString("region"))
		if err != nil {
			return nil, fmt.Errorf("failed to read secret from AWS Secrets Manager (destination=%s, tenant=%s)", destID, dest.TenantID)
		}

		if secretOutput.SecretString != nil {
			var secretMap map[string]string
			if err := json.Unmarshal([]byte(*secretOutput.SecretString), &secretMap); err != nil {
				return nil, fmt.Errorf("failed to parse secret value: malformed JSON (destination=%s, tenant=%s)", destID, dest.TenantID)
			}
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
	}

	return cfg, nil
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
		// nolint:gosec // CWE-532 false positive: credentials passed to AWS SDK, not logged
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
		case types.QueryExecutionStateQueued, types.QueryExecutionStateRunning:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}
