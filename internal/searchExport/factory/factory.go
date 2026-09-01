package factory

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
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
	QueryEngine       string
	Unload            query.UnloadExecutor
	RowStream         query.RowStreamExecutor
	Uploader          upload.CloudUploader
	ExportBucket      string
	LegacyMode        bool
	StagingBlobConfig *destination.AzureBlobConfig
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
	var exportBlob *destination.AzureBlobConfig

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
		exportBlob, err = destination.LoadAzureBlobConfig(ctx, db, destID, tenantID)
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

	deps := &ExportDeps{
		QueryEngine:  queryEngine,
		Uploader:     exportUploader,
		ExportBucket: exportBucket,
		LegacyMode:   legacyMode,
	}

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
				if store.Type == datastore.StoreTypeDatabahnStorage && store.DestinationID != nil {
					staging, err = destination.LoadDatabahnStorageStagingConfig(ctx, db, *store.DestinationID, tenantID)
				} else if store.Type == datastore.StoreTypeDatabahnStorage {
					return nil, fmt.Errorf("athena staging S3 config not resolved: DATABAHN_STORAGE data store has no linked destination")
				} else if isExternalAthenaStore(store) {
					return nil, fmt.Errorf("external Athena store %s missing staging credentials", dataStoreID)
				} else {
					return nil, fmt.Errorf("athena staging S3 config not resolved for store type %s", store.Type)
				}
			}
			if log != nil && store.ExternalSearchProvider != "" {
				log.Info("Resolved external Athena export store",
					zap.String("dataStoreType", store.Type),
					zap.String("externalSearchProvider", store.ExternalSearchProvider))
			}
		}
		if err != nil || staging == nil {
			return nil, fmt.Errorf("athena staging S3 config is required: %w", err)
		}
		athenaCfg, err := resolveAthenaClientConfig(cfg, staging)
		if err != nil {
			return nil, err
		}
		athenaExec := query.NewAthenaExecutor(athenaCfg)
		athenaExec.SetLogger(log)
		deps.Unload = athenaExec
	case models.QueryEngineKustoADX:
		if exportBlob == nil {
			return nil, fmt.Errorf("ADX export requires an Azure Blob destination, got %s", destType)
		}
		dataStoreID, err := uuid.Parse(cfg.DataStoreID)
		if err != nil {
			return nil, fmt.Errorf("invalid dataStoreId: %w", err)
		}
		store, err := datastore.LoadExportDataStore(ctx, db, dataStoreID, tenantID)
		if err != nil {
			return nil, err
		}
		if store.ADX == nil {
			return nil, fmt.Errorf("ADX store %s missing cluster credentials", dataStoreID)
		}
		adxExec := query.NewADXExecutor(query.ADXConfig{
			ClusterURI:   store.ADX.ClusterURI,
			Database:     firstNonEmpty(cfg.Database, store.ADX.Database),
			TenantID:     store.ADX.TenantID,
			ClientID:     store.ADX.ClientID,
			ClientSecret: store.ADX.ClientSecret,
			NamePrefix:   query.ADXNamePrefix(reportID),
			StagingBlob:  exportBlob,
		})
		adxExec.SetLogger(log)
		deps.Unload = adxExec
		// .export stages into the export destination's own container; the pipeline reads
		// the staged blobs back from there and cleans them up afterwards.
		deps.StagingBlobConfig = exportBlob
	case models.QueryEngineKustoLAW, models.QueryEngineKustoLake:
		dataStoreID, err := uuid.Parse(cfg.DataStoreID)
		if err != nil {
			return nil, fmt.Errorf("invalid dataStoreId: %w", err)
		}
		store, err := datastore.LoadExportDataStore(ctx, db, dataStoreID, tenantID)
		if err != nil {
			return nil, err
		}
		if store.Sentinel == nil {
			return nil, fmt.Errorf("Sentinel store %s missing workspace credentials", dataStoreID)
		}
		tier := firstNonEmpty(cfg.SentinelStorageTier(), store.Sentinel.StorageTier)
		sentinelExec, err := query.NewSentinelExecutor(query.SentinelConfig{
			WorkspaceID:   store.Sentinel.WorkspaceID,
			WorkspaceName: store.Sentinel.WorkspaceName,
			StorageTier:   tier,
			TenantID:      store.Sentinel.TenantID,
			ClientID:      store.Sentinel.ClientID,
			ClientSecret:  store.Sentinel.ClientSecret,
			QueryTimeout:  query.SentinelStreamOptionsForTier(tier).QueryTimeout,
			MaxRetries:    query.SentinelMaxRetriesFromEnv(),
		})
		if err != nil {
			return nil, err
		}
		sentinelExec.SetLogger(log)
		deps.RowStream = sentinelExec
		// No staging: Sentinel has no server-side export, so rows are pulled over the query
		// API and uploaded straight to the export destination — S3 or Azure Blob alike.
	case models.QueryEngineSynapse:
		dataStoreID, err := uuid.Parse(cfg.DataStoreID)
		if err != nil {
			return nil, fmt.Errorf("invalid dataStoreId: %w", err)
		}
		dataSetID, err := uuid.Parse(cfg.DataSetID)
		if err != nil {
			return nil, fmt.Errorf("invalid dataSetId: %w", err)
		}
		synapseSQL, err := datastore.LoadSynapseSQLConfig(ctx, db, dataStoreID, dataSetID, tenantID)
		if err != nil {
			return nil, err
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
			Workspace:      synapseSQL.Workspace,
			Database:       firstNonEmpty(cfg.Database, synapseSQL.Database, meta.Database),
			SqlUsername:    synapseSQL.SqlUsername,
			SqlPassword:    synapseSQL.SqlPassword,
			DataSourceName: dataSource,
		})
		synapseExec.SetLogger(log)
		deps.RowStream = synapseExec
		deps.StagingBlobConfig = exportBlob
	default:
		return nil, fmt.Errorf("unsupported query engine: %s", queryEngine)
	}

	return deps, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func isExternalAthenaStore(store *datastore.ExportDataStore) bool {
	if store == nil {
		return false
	}
	switch store.Type {
	case datastore.StoreTypeExternalStorage, datastore.StoreTypeDerivedDatastore:
		return datastore.IsExternalAthenaProvider(store.ExternalSearchProvider, nil)
	case datastore.StoreTypeDatabahnDestination:
		return store.ExternalSearchProvider == datastore.ExternalProviderSecurityLake
	default:
		return false
	}
}

// resolveAthenaClientConfig builds Athena client settings from store credentials, preferring
// the region from the export config when present. The output location is always derived
// locally — see athenaOutputLocationFor.
// isSecurityLakeProvider reports whether the export reads from AWS Security Lake, whether the
// store is external or a pipeline AWS_SECURITY_LAKE destination -- both stamp the same provider.
func isSecurityLakeProvider(cfg *models.SearchExportConfig) bool {
	return cfg != nil &&
		strings.EqualFold(strings.TrimSpace(cfg.ExternalSearchProvider), models.ProviderSecurityLake)
}

func resolveAthenaClientConfig(cfg *models.SearchExportConfig, staging *destination.S3Config) (query.AthenaConfig, error) {
	if staging == nil {
		return query.AthenaConfig{}, fmt.Errorf("athena staging config is required")
	}
	region := strings.TrimSpace(staging.Region)
	if cfg != nil && strings.TrimSpace(cfg.AthenaRegion()) != "" {
		region = strings.TrimSpace(cfg.AthenaRegion())
	}
	if region == "" {
		return query.AthenaConfig{}, fmt.Errorf("athena region is required")
	}

	outputLocation, err := athenaOutputLocationFor(staging)
	if err != nil {
		return query.AthenaConfig{}, err
	}
	// Validate at this assignment site, not only inside the builder: the URI is
	// passed to the Athena client and interpolated into UNLOAD SQL, so it must
	// be a canonical s3:// location for the staging bucket with no quotes or
	// control characters before it leaves this function.
	if err := validateAthenaOutputLocation(outputLocation, athenaOutputBucket.FindString(strings.TrimSpace(staging.Bucket))); err != nil {
		return query.AthenaConfig{}, err
	}

	return query.AthenaConfig{
		Region:          region,
		Workgroup:       "primary",
		OutputLocation:  outputLocation,
		AuthType:        staging.AuthType,
		AccessKeyID:     staging.AccessKeyID,
		SecretAccessKey: staging.SecretAccessKey,
		RoleArn:         staging.RoleArn,
		ExternalID:      staging.ExternalID,
		// Security Lake's OCSF catalog is the only Athena source known to expose
		// microsecond timestamps, which UNLOAD rejects. Every other Athena source
		// exports correctly as-is and is deliberately left on the untouched path.
		NarrowTimestamps: isSecurityLakeProvider(cfg),
	}, nil
}

// IsSupportedExportMatrix reports whether a query engine and export destination type can be wired together.
func IsSupportedExportMatrix(queryEngine, destType string) bool {
	dest := strings.ToUpper(destType)
	switch strings.ToUpper(queryEngine) {
	// These engines never write through the export destination while the query runs: Athena
	// and Synapse stage into their own storage, and Sentinel rows are encoded in the worker
	// and uploaded client-side. The destination is only the write target, so any type the
	// uploader supports works.
	case models.QueryEngineAthena, models.QueryEngineSynapse, models.QueryEngineKustoLAW, models.QueryEngineKustoLake:
		switch dest {
		case models.DestTypeS3, models.DestTypeS3Parquet, models.DestTypeAzureBlob:
			return true
		}
		return false
	case models.QueryEngineKustoADX:
		// ADX .export writes into the export destination's own blob container,
		// so Azure Blob is the only supported export destination.
		return dest == models.DestTypeAzureBlob
	default:
		return false
	}
}

// databahnAthenaOutputPrefix is the key Athena writes query results under, matching
// destination.S3Config.AthenaOutputLocation and backend AWSConstants.ATHENA_OUTPUT_PATH.
const databahnAthenaOutputPrefix = ".databahn_out"

// athenaOutputBucket is AWS's bucket naming rule: 3-63 characters of lowercase
// alphanumerics, dots and hyphens, starting and ending alphanumeric.
var athenaOutputBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$`)

// athenaOutputLocation matches a canonical s3://<bucket>/<key> URI. The bucket
// capture is AWS's naming rule; the optional key cannot contain whitespace,
// quotes, backticks, or backslashes.
var athenaOutputLocation = regexp.MustCompile(`^s3://([a-z0-9][a-z0-9.\-]{1,61}[a-z0-9])(/[^\s"'` + "`" + `\\]*)?$`)

// athenaOutputLocationFor builds Athena's result URI from the staging bucket name.
// searchExportConfig.athenaOutputLocation is not read. The URI is assembled from the
// allowlisted bucket match via net/url so the original string never enters the client.
func athenaOutputLocationFor(staging *destination.S3Config) (string, error) {
	if staging == nil {
		return "", fmt.Errorf("athena staging config is required")
	}
	safeBucket := athenaOutputBucket.FindString(strings.TrimSpace(staging.Bucket))
	if safeBucket == "" {
		return "", fmt.Errorf("athena staging bucket %q is not a valid S3 bucket name", staging.Bucket)
	}
	location := (&url.URL{Scheme: "s3", Host: safeBucket, Path: "/" + databahnAthenaOutputPrefix}).String()
	if err := validateAthenaOutputLocation(location, safeBucket); err != nil {
		return "", err
	}
	return location, nil
}

func validateAthenaOutputLocation(location, authorizedBucket string) error {
	if strings.ContainsFunc(location, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return fmt.Errorf("athena output location contains control characters")
	}
	if strings.ContainsAny(location, `'"\`) {
		return fmt.Errorf("athena output location contains quotes or escape characters")
	}
	match := athenaOutputLocation.FindStringSubmatch(location)
	if match == nil {
		return fmt.Errorf("athena output location %q is not a canonical s3://bucket/key URI", location)
	}
	if authorizedBucket == "" {
		return fmt.Errorf("cannot authorize athena output location %q: staging bucket is not configured", location)
	}
	if match[1] != authorizedBucket {
		return fmt.Errorf(
			"athena output location %q targets bucket %q, which is not the authorized staging bucket %q",
			location, match[1], authorizedBucket)
	}
	return nil
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
