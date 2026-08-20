package databahnstorage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/apply"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/validate"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// ApplyDataCatalogToAthena reads unapplied fields from data_catalog, looks up the
// corresponding Athena table, runs ALTER TABLE ADD COLUMNS, and marks them as applied.
func ApplyDataCatalogToAthena(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError

	fields, err := queryUnappliedFields(ctx)
	if err != nil {
		return common.NewJobResultFromError(fmt.Errorf("failed to query data_catalog: %w", err))
	}

	if len(fields) == 0 {
		logger.GetLoggerWithContext(ctx).Info("no unapplied data catalog fields found")
		return common.NewJobResultSuccess()
	}

	logger.GetLoggerWithContext(ctx).Info("found unapplied data catalog fields", zap.Int("count", len(fields)))

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
				logger.GetLoggerWithContext(ctx).Warn("skipping group — Athena table or config not found, will retry on next run",
					append(model.GroupFields(first.DestID, first.SourceID, first.TenantID, model.DispenserDatabahnStorage),
						zap.String("reason", err.Error()))...)
				continue
			}

			logger.GetLoggerWithContext(ctx).Error("failed to apply catalog fields to Athena",
				append(model.GroupFields(first.DestID, first.SourceID, first.TenantID, model.DispenserDatabahnStorage),
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
	groupFields := model.GroupFields(destID, sourceID, tenantID, model.DispenserDatabahnStorage)
	log.Info("processing catalog schema sync group",
		append(groupFields, zap.Int("field_count", len(fields)))...)

	target, err := resolveDatabahnStorageTarget(ctx, destID, sourceID)
	if err != nil {
		return err
	}

	valid, err := cleanFields(ctx, destID, sourceID, tenantID, model.DispenserDatabahnStorage, fields)
	if err != nil {
		return err
	}
	if len(valid) == 0 {
		return nil
	}

	database := model.DatabaseName(tenantID)
	schemaOps := ops
	if schemaOps == nil {
		defaultOps := apply.PlatformOps(target.region, database, target.tableName, target.outputLocation)
		schemaOps = &defaultOps
	}
	return runPreflight(
		ctx, destID, sourceID, tenantID, model.DispenserDatabahnStorage,
		valid, database, target.tableName, target.region, *schemaOps,
	)
}

var (
	resolveDatabahnStorageTarget = defaultResolveDatabahnStorageTarget
	cleanFields                  = validate.CleanFields
	runPreflight                 = apply.WithPreflight
	queryUnappliedFields         = defaultQueryUnappliedFields
	processGroupFn               = processGroup
)

func defaultQueryUnappliedFields(ctx context.Context) ([]model.Field, error) {
	var fields []model.Field
	err := config.GetDB().WithContext(ctx).
		Where("applied_on_search = ? AND dispenser_type = ?", false, model.DispenserDatabahnStorage).
		Find(&fields).Error
	return fields, err
}

type databahnStorageTarget struct {
	tableName      string
	region         string
	outputLocation string
}

func defaultResolveDatabahnStorageTarget(ctx context.Context, destID, sourceID uuid.UUID) (databahnStorageTarget, error) {
	db := config.GetDB()

	var storeIDStr string
	err := db.WithContext(ctx).Raw(
		"SELECT id FROM search_data_store WHERE destination_id = ? AND type = 'DATABAHN_STORAGE' LIMIT 1",
		destID,
	).Scan(&storeIDStr).Error
	if err != nil || storeIDStr == "" {
		return databahnStorageTarget{}, model.NewTableNotFoundError("search_data_store not found for destination %s: %v", destID, err)
	}

	var searchConfigJSON string
	err = db.WithContext(ctx).Raw(
		"SELECT search_configuration FROM search_data_set WHERE store_id = ? AND source_id = ? LIMIT 1",
		storeIDStr, sourceID,
	).Scan(&searchConfigJSON).Error
	if err != nil || searchConfigJSON == "" {
		return databahnStorageTarget{}, model.NewTableNotFoundError("search_data_set not found for store %s, source %s: %v", storeIDStr, sourceID, err)
	}
	return parseDatabahnStorageSearchConfig(storeIDStr, sourceID, searchConfigJSON)
}

func parseDatabahnStorageSearchConfig(storeIDStr string, sourceID uuid.UUID, searchConfigJSON string) (databahnStorageTarget, error) {
	var sc model.SearchConfig
	if err := json.Unmarshal([]byte(searchConfigJSON), &sc); err != nil {
		return databahnStorageTarget{}, fmt.Errorf("failed to parse search_configuration: %w", err)
	}
	tableName := sc.S3Configuration.AthenaTable
	if tableName == "" {
		return databahnStorageTarget{}, model.NewTableNotFoundError("athenaTable is empty in search_configuration for store %s, source %s", storeIDStr, sourceID)
	}
	region := sc.S3Configuration.DatabahnStorageRegion
	if region == "" {
		return databahnStorageTarget{}, fmt.Errorf("databahnStorageRegion is empty in search_configuration for store %s, source %s", storeIDStr, sourceID)
	}
	bucket := model.BucketFromS3Location(sc.S3Configuration.S3Location)
	if bucket == "" {
		return databahnStorageTarget{}, fmt.Errorf("could not extract bucket from s3Location for store %s, source %s", storeIDStr, sourceID)
	}
	return databahnStorageTarget{
		tableName:      tableName,
		region:         region,
		outputLocation: fmt.Sprintf("s3://%s/athena-results/", bucket),
	}, nil
}
