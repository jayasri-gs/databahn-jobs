package factory

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/models"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"github.com/databahn-ai/databahn-jobs/internal/store/datastore"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ExportDeps struct {
	Executor     query.QueryExecutor
	Uploader     upload.CloudUploader
	ExportBucket string
	LegacyMode   bool
}

func NewExportDeps(ctx context.Context, db *gorm.DB, cfg *models.SearchExportConfig, tenantID uuid.UUID, reportID string, log *zap.Logger) (*ExportDeps, error) {
	if cfg == nil {
		return nil, fmt.Errorf("export config is required")
	}

	queryEngine := strings.ToUpper(cfg.QueryEngine)
	legacyMode := queryEngine == ""
	if legacyMode {
		queryEngine = models.QueryEngineAthena
	}

	destID, err := uuid.Parse(cfg.DestinationID)
	if err != nil {
		return nil, fmt.Errorf("invalid destinationId: %w", err)
	}

	destType := strings.ToUpper(cfg.DestinationType)
	if destType == "" {
		s3Cfg, s3Err := destination.LoadS3Config(ctx, db, destID, tenantID)
		if s3Err == nil && s3Cfg != nil {
			destType = models.DestTypeS3
		} else {
			blobCfg, blobErr := destination.LoadAzureBlobConfig(ctx, db, destID, tenantID)
			if blobErr == nil && blobCfg != nil {
				destType = models.DestTypeAzureBlob
			} else if s3Err != nil {
				return nil, s3Err
			} else if blobErr != nil {
				return nil, blobErr
			}
		}
	}

	var exportUploader upload.CloudUploader
	var exportBucket string

	switch destType {
	case models.DestTypeS3, models.DestTypeS3Parquet:
		exportS3, err := destination.LoadS3Config(ctx, db, destID, tenantID)
		if err != nil {
			return nil, err
		}
		awsCfg, err := awsConfigFromS3(ctx, exportS3)
		if err != nil {
			return nil, err
		}
		exportUploader = upload.NewS3Uploader(awsCfg, "export-expiry=true")
		exportBucket = exportS3.Bucket
	case models.DestTypeAzureBlob:
		exportBlob, err := destination.LoadAzureBlobConfig(ctx, db, destID, tenantID)
		if err != nil {
			return nil, err
		}
		client, err := destination.NewAzureBlobClient(exportBlob)
		if err != nil {
			return nil, err
		}
		exportUploader = upload.NewAzureUploader(client, exportBlob)
		exportBucket = exportBlob.Container
	default:
		return nil, fmt.Errorf("unsupported export destination type: %s", destType)
	}

	var executor query.QueryExecutor
	switch queryEngine {
	case models.QueryEngineAthena:
		var staging *destination.S3Config
		if legacyMode {
			staging, err = destination.LoadS3Config(ctx, db, destID, tenantID)
		} else {
			dataStoreID, err := uuid.Parse(cfg.DataStoreID)
			if err != nil {
				return nil, fmt.Errorf("invalid dataStoreId: %w", err)
			}
			store, err := datastore.LoadExportDataStore(ctx, db, dataStoreID, tenantID)
			if err != nil {
				return nil, err
			}
			staging = store.StagingS3
			if staging == nil {
				staging, err = destination.LoadS3Config(ctx, db, destID, tenantID)
			}
		}
		if err != nil || staging == nil {
			return nil, fmt.Errorf("athena staging S3 config is required: %w", err)
		}
		athenaExec := query.NewAthenaExecutor(query.AthenaConfig{
			Region:          staging.Region,
			Workgroup:       "primary",
			OutputLocation:  staging.AthenaOutputLocation(),
			AuthType:        staging.AuthType,
			AccessKeyID:     staging.AccessKeyID,
			SecretAccessKey: staging.SecretAccessKey,
			RoleArn:         staging.RoleArn,
			ExternalID:      staging.ExternalID,
		})
		athenaExec.SetLogger(log)
		executor = athenaExec
	case models.QueryEngineSynapse:
		dataStoreID, err := uuid.Parse(cfg.DataStoreID)
		if err != nil {
			return nil, fmt.Errorf("invalid dataStoreId: %w", err)
		}
		dataSetID, err := uuid.Parse(cfg.DataSetID)
		if err != nil {
			return nil, fmt.Errorf("invalid dataSetId: %w", err)
		}
		store, err := datastore.LoadExportDataStore(ctx, db, dataStoreID, tenantID)
		if err != nil {
			return nil, err
		}
		if store.SynapseSQL == nil || store.StagingBlob == nil {
			return nil, fmt.Errorf("synapse export requires SQL and staging blob configuration")
		}
		meta, err := datastore.LoadSynapseExportMetadata(ctx, db, dataSetID, tenantID)
		if err != nil {
			return nil, err
		}
		dataSource := cfg.SynapseDataSourceName
		if dataSource == "" {
			dataSource = meta.DataSourceName
		}
		synapseExec := query.NewSynapseExecutor(query.SynapseConfig{
			Workspace:        store.SynapseSQL.Workspace,
			Database:         firstNonEmpty(cfg.Database, store.SynapseSQL.Database, meta.Database),
			SqlUsername:      store.SynapseSQL.SqlUsername,
			SqlPassword:      store.SynapseSQL.SqlPassword,
			DataSourceName:   dataSource,
			StagingContainer: store.StagingBlob.Container,
			StagingBlob:      store.StagingBlob,
		})
		synapseExec.SetLogger(log)
		executor = synapseExec
	default:
		return nil, fmt.Errorf("unsupported query engine: %s", queryEngine)
	}

	return &ExportDeps{
		Executor:     executor,
		Uploader:     exportUploader,
		ExportBucket: exportBucket,
		LegacyMode:   legacyMode,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// IsSupportedExportMatrix reports whether a query engine and export destination type can be wired together.
func IsSupportedExportMatrix(queryEngine, destType string) bool {
	switch strings.ToUpper(queryEngine) {
	case models.QueryEngineAthena, models.QueryEngineSynapse:
	default:
		return false
	}
	switch strings.ToUpper(destType) {
	case models.DestTypeS3, models.DestTypeS3Parquet, models.DestTypeAzureBlob:
		return true
	default:
		return false
	}
}

func awsConfigFromS3(ctx context.Context, cfg *destination.S3Config) (aws.Config, error) {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(cfg.Region))
	if cfg.AuthType == "role_based" {
		baseCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
		if err != nil {
			return aws.Config{}, err
		}
		stsClient := sts.NewFromConfig(baseCfg)
		opts = append(opts, awsconfig.WithCredentialsProvider(
			stscreds.NewAssumeRoleProvider(stsClient, cfg.RoleArn, func(o *stscreds.AssumeRoleOptions) {
				if cfg.ExternalID != "" {
					o.ExternalID = aws.String(cfg.ExternalID)
				}
			}),
		))
	} else if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}
