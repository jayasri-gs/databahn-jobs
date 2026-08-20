package s3parquet

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"

	dbaws "github.com/databahn-ai/common-utils/aws"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/apply"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/validate"
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

// ApplyS3ParquetCatalogToAthena syncs unapplied S3 Parquet catalog fields to customer Athena tables.
func ApplyS3ParquetCatalogToAthena(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError

	fields, err := queryUnappliedFields(ctx)
	if err != nil {
		return common.NewJobResultFromError(fmt.Errorf("failed to query data_catalog for s3parquet: %w", err))
	}

	if len(fields) == 0 {
		logger.GetLoggerWithContext(ctx).Info("no unapplied s3 parquet data catalog fields found")
		return common.NewJobResultSuccess()
	}

	logger.GetLoggerWithContext(ctx).Info("found unapplied s3 parquet data catalog fields", zap.Int("count", len(fields)))

	groups := make(map[string][]model.Field)
	for _, f := range fields {
		key := model.GroupKey(f.DestID, f.SourceID, f.TenantID)
		groups[key] = append(groups[key], f)
	}

	for _, group := range groups {
		first := group[0]
		err := processGroupFn(ctx, first.DestID, first.SourceID, first.TenantID, group, nil)
		if err != nil {
			if model.IsTableNotFound(err) {
				logger.GetLoggerWithContext(ctx).Warn("skipping s3 parquet group — Athena table or config not found, will retry on next run",
					append(model.GroupFields(first.DestID, first.SourceID, first.TenantID, model.DispenserS3Parquet),
						zap.Error(err))...)
				continue
			}

			logger.GetLoggerWithContext(ctx).Error("failed to apply s3 parquet catalog fields to Athena",
				append(model.GroupFields(first.DestID, first.SourceID, first.TenantID, model.DispenserS3Parquet),
					zap.Error(err))...)
			jobErrors = append(jobErrors, common.JobError{Message: err.Error()})
		}
	}

	if len(jobErrors) > 0 {
		return common.NewJobResultFromErrors(jobErrors)
	}
	return common.NewJobResultSuccess()
}

func processGroup(ctx context.Context, destID, sourceID, tenantID uuid.UUID, fields []model.Field, ops *apply.SchemaOps) error {
	log := logger.GetLoggerWithContext(ctx)
	groupFields := model.GroupFields(destID, sourceID, tenantID, model.DispenserS3Parquet)
	log.Info("processing catalog schema sync group",
		append(groupFields, zap.Int("field_count", len(fields)))...)

	target, err := resolveS3ParquetTarget(ctx, destID, sourceID, tenantID)
	if err != nil {
		return err
	}

	valid, err := cleanFields(ctx, destID, sourceID, tenantID, model.DispenserS3Parquet, fields)
	if err != nil {
		return err
	}
	if len(valid) == 0 {
		return nil
	}

	database := model.DatabaseName(tenantID)
	schemaOps := ops
	if schemaOps == nil {
		athenaClient, err := createAthenaClient(ctx, target.destConfig)
		if err != nil {
			log.Error("failed to create customer Athena client",
				append(groupFields,
					zap.String("auth_type", target.destConfig.AuthType),
					zap.String("region", target.destConfig.Region),
					zap.Error(err))...)
			return fmt.Errorf("failed to create customer Athena client for %s: %w", destID, err)
		}
		log.Info("customer Athena client created",
			append(groupFields,
				zap.String("auth_type", target.destConfig.AuthType),
				zap.String("region", target.destConfig.Region))...)
		defaultOps := apply.CustomerOps(athenaClient, database, target.tableName, target.outputLocation)
		schemaOps = &defaultOps
	}

	return runPreflight(ctx, apply.PreflightParams{
		DestID:        destID,
		SourceID:      sourceID,
		TenantID:      tenantID,
		DispenserType: model.DispenserS3Parquet,
		Valid:         valid,
		Database:      database,
		TableName:     target.tableName,
		Region:        target.destConfig.Region,
	}, *schemaOps)
}

var (
	resolveS3ParquetTarget = defaultResolveS3ParquetTarget
	getDestConfig          = getDestinationConfig
	createAthenaClient     = createCustomerAthenaClient
	cleanFields            = validate.CleanFields
	runPreflight           = apply.WithPreflight
	queryUnappliedFields   = defaultQueryUnappliedS3ParquetFields
	processGroupFn         = processGroup
	readSecretByName       = dbaws.ReadSecretByName
	appRegion              = defaultAppRegion
)

func defaultAppRegion() string {
	return appConfig.GetAppConfiguration().GetString("region")
}

func defaultQueryUnappliedS3ParquetFields(ctx context.Context) ([]model.Field, error) {
	var fields []model.Field
	err := appConfig.GetDB().WithContext(ctx).
		Where("applied_on_search = ? AND dispenser_type = ?", false, model.DispenserS3Parquet).
		Find(&fields).Error
	return fields, err
}

type s3ParquetTarget struct {
	tableName      string
	outputLocation string
	destConfig     *destinationConfig
}

func defaultResolveS3ParquetTarget(ctx context.Context, destID, sourceID, tenantID uuid.UUID) (s3ParquetTarget, error) {
	db := appConfig.GetDB()

	var storeIDStr string
	err := db.WithContext(ctx).Raw(
		"SELECT id FROM search_data_store WHERE destination_id = ? AND type = 'DATABAHN_DESTINATION' LIMIT 1",
		destID,
	).Scan(&storeIDStr).Error
	if err != nil || storeIDStr == "" {
		return s3ParquetTarget{}, model.NewTableNotFoundError("search_data_store not found for s3 parquet destination %s", destID)
	}

	var searchConfigJSON string
	err = db.WithContext(ctx).Raw(
		"SELECT search_configuration FROM search_data_set WHERE store_id = ? AND source_id = ? LIMIT 1",
		storeIDStr, sourceID,
	).Scan(&searchConfigJSON).Error
	if err != nil || searchConfigJSON == "" {
		return s3ParquetTarget{}, model.NewTableNotFoundError("search_data_set not found for s3 parquet store %s, source %s", storeIDStr, sourceID)
	}

	tableName, err := parseS3ParquetTableName(storeIDStr, sourceID, searchConfigJSON)
	if err != nil {
		return s3ParquetTarget{}, err
	}

	destConfig, err := getDestConfig(ctx, destID, tenantID)
	if err != nil {
		return s3ParquetTarget{}, fmt.Errorf("failed to get destination config for %s: %w", destID, err)
	}
	if destConfig.Region == "" {
		return s3ParquetTarget{}, fmt.Errorf("region is empty in destination config for %s", destID)
	}

	return s3ParquetTarget{
		tableName:      tableName,
		outputLocation: fmt.Sprintf("s3://%s/.databahn_out/", destConfig.Bucket),
		destConfig:     destConfig,
	}, nil
}

func parseS3ParquetTableName(storeIDStr string, sourceID uuid.UUID, searchConfigJSON string) (string, error) {
	var sc model.SearchConfig
	if err := json.Unmarshal([]byte(searchConfigJSON), &sc); err != nil {
		return "", fmt.Errorf("failed to parse search_configuration: %w", err)
	}
	tableName := sc.S3Configuration.AthenaTable
	if tableName == "" {
		return "", model.NewTableNotFoundError("athenaTable is empty for s3 parquet store %s, source %s", storeIDStr, sourceID)
	}
	return tableName, nil
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

	cfg, err := parseDestinationWrapperConfiguration(dest.Configuration)
	if err != nil {
		return nil, err
	}

	// nolint:gosec // CWE-532 false positive: taint analysis flags variable names, but no secrets are logged
	if cfg.secretID != "" {
		var backendSecretID string
		err := db.WithContext(ctx).Raw(
			"SELECT backend_secret_id FROM secrets WHERE id = ? LIMIT 1",
			cfg.secretID,
		).Scan(&backendSecretID).Error
		if err != nil || backendSecretID == "" {
			return nil, fmt.Errorf("failed to look up backend_secret_id: secret reference not found or inaccessible (destination=%s, tenant=%s, bucket=%s)", destID, dest.TenantID, cfg.config.Bucket)
		}

		secretOutput, err := readSecretByName(backendSecretID, appRegion())
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
					cfg.config.AccessKeyID = v
				case "secret_access_key":
					cfg.config.SecretKey = v
				case "role_arn":
					cfg.config.RoleArn = v
				case "external_id":
					cfg.config.ExternalID = v
				}
			}
		}
	}

	return cfg.config, nil
}

type parsedDestinationConfig struct {
	config   *destinationConfig
	secretID string
}

func parseDestinationWrapperConfiguration(configuration string) (*parsedDestinationConfig, error) {
	var wrapper destinationConfigWrapper
	if err := json.Unmarshal([]byte(configuration), &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse destination configuration: %w", err)
	}

	return &parsedDestinationConfig{
		config: &destinationConfig{
			AuthType:    wrapper.getString("auth_type"),
			AccessKeyID: wrapper.getString("access_key_id"),
			SecretKey:   wrapper.getString("secret_access_key"),
			RoleArn:     wrapper.getString("role_arn"),
			ExternalID:  wrapper.getString("external_id"),
			Region:      wrapper.getString("region"),
			Bucket:      wrapper.getString("bucket"),
		},
		secretID: wrapper.SecretID,
	}, nil
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
