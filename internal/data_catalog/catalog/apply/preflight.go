package apply

import (
	"context"
	"fmt"

	athenastore "github.com/databahn-ai/databahn-jobs/internal/store/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// SchemaOps provides DESCRIBE and DDL operations for schema sync.
type SchemaOps struct {
	Describe func(ctx context.Context) (map[string]struct{}, string, error)
	RunDDL   func(ctx context.Context, query string) error
}

// PreflightParams groups inputs for WithPreflight.
type PreflightParams struct {
	DestID, SourceID, TenantID uuid.UUID
	DispenserType              string
	Valid                      []model.Field
	Database, TableName, Region string
}

// WithPreflight validates Athena presence, marks already-present rows,
// runs ALTER TABLE for missing columns, then marks newly-added rows.
func WithPreflight(ctx context.Context, p PreflightParams, ops SchemaOps) error {
	destID, sourceID, tenantID := p.DestID, p.SourceID, p.TenantID
	dispenserType := p.DispenserType
	valid := p.Valid
	database, tableName, region := p.Database, p.TableName, p.Region
	log := logger.GetLoggerWithContext(ctx)
	groupFields := model.GroupFields(destID, sourceID, tenantID, dispenserType)

	existingCols, executionID, err := ops.Describe(ctx)
	if err != nil {
		if athenastore.IsTableNotFound(err) {
			return model.NewTableNotFoundError("%s", err.Error())
		}
		log.Error("failed to fetch Athena table schema",
			append(groupFields, zap.String("database", database), zap.String("table", tableName), zap.String("region", region), zap.Error(err))...)
		return fmt.Errorf("failed to DESCRIBE %s.%s: %w", database, tableName, err)
	}

	log.Info("Athena table schema fetched",
		append(groupFields,
			zap.String("database", database),
			zap.String("table", tableName),
			zap.String("region", region),
			zap.String("describe_execution_id", executionID),
			zap.Int("existing_column_count", len(existingCols)),
		)...)

	if len(existingCols) == 0 {
		log.Warn("DESCRIBE returned zero columns — table may be empty or parse issue",
			append(groupFields, zap.String("database", database), zap.String("table", tableName))...)
	}

	existingNames := make([]string, 0, len(existingCols))
	for name := range existingCols {
		existingNames = append(existingNames, name)
	}
	log.Debug("existing Athena columns", append(groupFields, zap.Strings("column_names", existingNames))...)

	alreadyPresent, missing := PartitionByPresence(valid, existingCols)
	log.Info("catalog fields partitioned by Athena presence",
		append(groupFields,
			zap.Int("already_present_count", len(alreadyPresent)),
			zap.Int("missing_count", len(missing)),
			zap.Strings("already_present_names", model.FieldNames(alreadyPresent)),
			zap.Strings("missing_names", model.FieldNames(missing)),
		)...)

	if len(alreadyPresent) > 0 {
		if err := MarkApplied(ctx, model.FieldIDs(alreadyPresent), "already_present"); err != nil {
			return err
		}
		log.Info("marked already-present catalog fields applied",
			append(groupFields,
				zap.Int64s("catalog_ids", model.FieldIDs(alreadyPresent)),
				zap.Int("count", len(alreadyPresent)),
			)...)
	}

	if len(missing) == 0 {
		log.Info("all catalog fields already present in Athena, skipping DDL",
			append(groupFields,
				zap.String("database", database),
				zap.String("table", tableName),
				zap.Int("count", len(alreadyPresent)),
			)...)
		return nil
	}

	for _, f := range missing {
		log.Debug("mapping catalog field to Athena type",
			append(groupFields,
				zap.String("field_name", f.Name),
				zap.String("field_type", f.FieldType),
				zap.String("athena_type", model.AthenaType(f.FieldType)),
			)...)
	}

	query, err := BuildAddColumnsDDL(database, tableName, missing)
	if err != nil {
		return err
	}

	log.Info("executing Athena ALTER TABLE",
		append(groupFields,
			zap.String("database", database),
			zap.String("table", tableName),
			zap.String("region", region),
			zap.Int("columns", len(missing)),
			zap.String("query", query),
		)...)

	if err := ops.RunDDL(ctx, query); err != nil {
		log.Error("ALTER TABLE failed",
			append(groupFields,
				zap.String("database", database),
				zap.String("table", tableName),
				zap.String("query", query),
				zap.Error(err),
			)...)
		return fmt.Errorf("ALTER TABLE failed for %s.%s: %w", database, tableName, err)
	}

	log.Info("Athena ALTER TABLE succeeded",
		append(groupFields,
			zap.String("database", database),
			zap.String("table", tableName),
			zap.Int("fields_added", len(missing)),
		)...)

	if err := MarkApplied(ctx, model.FieldIDs(missing), "after_ddl"); err != nil {
		return err
	}
	log.Info("marked newly-added catalog fields applied",
		append(groupFields,
			zap.Int64s("catalog_ids", model.FieldIDs(missing)),
			zap.Int("count", len(missing)),
		)...)

	return nil
}

// PlatformOps returns Athena operations using platform credentials in a region.
func PlatformOps(region, database, tableName, outputLocation string) SchemaOps {
	return SchemaOps{
		Describe: func(ctx context.Context) (map[string]struct{}, string, error) {
			return athenastore.GetTableColumnNamesInRegion(ctx, region, database, tableName, outputLocation)
		},
		RunDDL: func(ctx context.Context, query string) error {
			return athenastore.RunDDLInRegion(ctx, query, region, outputLocation)
		},
	}
}

// CustomerOps returns Athena operations using a customer-provided client.
func CustomerOps(client *athena.Client, database, tableName, outputLocation string) SchemaOps {
	return SchemaOps{
		Describe: func(ctx context.Context) (map[string]struct{}, string, error) {
			return athenastore.GetTableColumnNames(ctx, client, database, tableName, outputLocation)
		},
		RunDDL: func(ctx context.Context, query string) error {
			return athenastore.RunDDL(ctx, client, query, outputLocation)
		},
	}
}
