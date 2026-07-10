package data_catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/athena"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type catalogField struct {
	ID        int64     `gorm:"column:id"`
	Name      string    `gorm:"column:name"`
	FieldType string    `gorm:"column:field_type"`
	SourceID  uuid.UUID `gorm:"column:source_id"`
	DestID    uuid.UUID `gorm:"column:destination_id"`
	TenantID  uuid.UUID `gorm:"column:tenant_id"`
}

func (catalogField) TableName() string { return "data_catalog" }

type searchConfig struct {
	S3Configuration struct {
		AthenaTable           string `json:"athenaTable"`
		S3Location            string `json:"s3Location"`
		DatabahnStorageRegion string `json:"databahnStorageRegion"`
	} `json:"s3Configuration"`
}

// bucketFromS3Location extracts the bucket name from an S3 URI like "s3://bucket/path/".
func bucketFromS3Location(s3Location string) string {
	trimmed := strings.TrimPrefix(s3Location, "s3://")
	if idx := strings.Index(trimmed, "/"); idx > 0 {
		return trimmed[:idx]
	}
	return trimmed
}

// groupKey uniquely identifies a destination+source+tenant combination.
func groupKey(destID, sourceID, tenantID uuid.UUID) string {
	return destID.String() + "|" + sourceID.String() + "|" + tenantID.String()
}

// athenaType converts backend-service field types to Athena SQL types.
func athenaType(fieldType string) string {
	switch fieldType {
	case "long":
		return "bigint"
	case "string":
		return "string"
	case "double":
		return "double"
	case "boolean":
		return "boolean"
	default:
		return "string"
	}
}

// tableNotFoundError is a non-fatal error indicating that the Athena table or
// its configuration is missing.  The group should be skipped with a warning.
type tableNotFoundError struct{ msg string }

func (e *tableNotFoundError) Error() string { return e.msg }

func newTableNotFoundError(format string, args ...interface{}) *tableNotFoundError {
	return &tableNotFoundError{msg: fmt.Sprintf(format, args...)}
}

// isTableNotFound returns true when the error signals a missing Athena table or
// configuration—these are expected and should not fail the whole job.
func isTableNotFound(err error) bool {
	if err == nil {
		return false
	}
	var tnf *tableNotFoundError
	if errors.As(err, &tnf) {
		return true
	}
	// Catch Athena-level "table not found" messages returned by the query engine.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "table_not_found") ||
		strings.Contains(msg, "table not found") ||
		strings.Contains(msg, "does not exist")
}

// databaseName returns the Athena database name for a tenant.
func databaseName(tenantID uuid.UUID) string {
	return "databahn_tenant_" + strings.ReplaceAll(tenantID.String(), "-", "_")
}

// ApplyDataCatalogToAthena reads unapplied fields from data_catalog, looks up the
// corresponding Athena table, runs ALTER TABLE ADD COLUMNS, and marks them as applied.
func ApplyDataCatalogToAthena(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	db := config.GetDB()

	// 1. Fetch unapplied catalog fields for Databahn Storage
	var fields []catalogField
	err := db.WithContext(ctx).
		Where("applied_on_search = ? AND dispenser_type = ?", false, "databahnstorage").
		Find(&fields).Error
	if err != nil {
		return common.NewJobResultFromError(fmt.Errorf("failed to query data_catalog: %w", err))
	}

	if len(fields) == 0 {
		logger.GetLoggerWithContext(ctx).Info("no unapplied data catalog fields found")
		return common.NewJobResultSuccess()
	}

	logger.GetLoggerWithContext(ctx).Info("found unapplied data catalog fields", zap.Int("count", len(fields)))

	// 2. Group by destination_id + source_id + tenant_id
	groups := make(map[string][]catalogField)
	for _, f := range fields {
		key := groupKey(f.DestID, f.SourceID, f.TenantID)
		groups[key] = append(groups[key], f)
	}

	// 3. Process each group
	for _, group := range groups {
		first := group[0]
		err := processGroup(ctx, first.DestID, first.SourceID, first.TenantID, group)
		if err != nil {
			if isTableNotFound(err) {
				// Athena table or configuration not found for this destination/source.
				// This is expected when the table hasn't been created yet — skip gracefully.
				logger.GetLoggerWithContext(ctx).Warn("skipping group — Athena table or config not found, will retry on next run",
					zap.String("destination_id", first.DestID.String()),
					zap.String("source_id", first.SourceID.String()),
					zap.String("tenant_id", first.TenantID.String()),
					zap.String("reason", err.Error()))
				continue
			}

			logger.GetLoggerWithContext(ctx).Error("failed to apply catalog fields to Athena",
				zap.String("destination_id", first.DestID.String()),
				zap.String("source_id", first.SourceID.String()),
				zap.String("tenant_id", first.TenantID.String()),
				zap.Error(err))
			jobErrors = append(jobErrors, common.JobError{Message: err.Error()})
			continue
		}

		// Mark as applied
		ids := make([]int64, len(group))
		for i, f := range group {
			ids[i] = f.ID
		}
		if err := config.GetDB().WithContext(ctx).
			Table("data_catalog").
			Where("id IN ?", ids).
			Updates(map[string]interface{}{
				"applied_on_search": true,
				"search_engine":     "ATHENA",
			}).Error; err != nil {
			logger.GetLoggerWithContext(ctx).Error("failed to mark catalog fields as applied",
				zap.Error(err))
			jobErrors = append(jobErrors, common.JobError{Message: fmt.Sprintf("failed to update applied_on_search: %v", err)})
		}
	}

	if len(jobErrors) > 0 {
		return common.NewJobResultFromErrors(jobErrors)
	}
	return common.NewJobResultSuccess()
}

// processGroup looks up the Athena table for a destination+source pair and runs ALTER TABLE.
func processGroup(ctx context.Context, destID, sourceID, tenantID uuid.UUID, fields []catalogField) error {
	db := config.GetDB()

	// Look up store id
	var storeIDStr string
	err := db.WithContext(ctx).Raw(
		"SELECT id FROM search_data_store WHERE destination_id = ? AND type = 'DATABAHN_STORAGE' LIMIT 1",
		destID,
	).Scan(&storeIDStr).Error
	if err != nil || storeIDStr == "" {
		return newTableNotFoundError("search_data_store not found for destination %s: %v", destID, err)
	}

	// Look up search_configuration JSON
	var searchConfigJSON string
	err = db.WithContext(ctx).Raw(
		"SELECT search_configuration FROM search_data_set WHERE store_id = ? AND source_id = ? LIMIT 1",
		storeIDStr, sourceID,
	).Scan(&searchConfigJSON).Error
	if err != nil || searchConfigJSON == "" {
		return newTableNotFoundError("search_data_set not found for store %s, source %s: %v", storeIDStr, sourceID, err)
	}

	// Parse to get Athena table name, region, and bucket
	var sc searchConfig
	if err := json.Unmarshal([]byte(searchConfigJSON), &sc); err != nil {
		return fmt.Errorf("failed to parse search_configuration: %w", err)
	}
	tableName := sc.S3Configuration.AthenaTable
	if tableName == "" {
		return newTableNotFoundError("athenaTable is empty in search_configuration for store %s, source %s", storeIDStr, sourceID)
	}
	region := sc.S3Configuration.DatabahnStorageRegion
	if region == "" {
		return fmt.Errorf("databahnStorageRegion is empty in search_configuration for store %s, source %s", storeIDStr, sourceID)
	}
	bucket := bucketFromS3Location(sc.S3Configuration.S3Location)
	if bucket == "" {
		return fmt.Errorf("could not extract bucket from s3Location for store %s, source %s", storeIDStr, sourceID)
	}
	outputLocation := fmt.Sprintf("s3://%s/athena-results/", bucket)

	// Validate fields: drop leading-underscore, reserved-keyword, and duplicate
	// names. Duplicates are compared against columns already applied to this
	// table so an existing Athena column is never re-added.
	var existingApplied []string
	if err := db.WithContext(ctx).
		Table("data_catalog").
		Where("applied_on_search = ? AND destination_id = ? AND source_id = ? AND tenant_id = ? AND dispenser_type = ?",
			true, destID, sourceID, tenantID, "databahnstorage").
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
		logger.GetLoggerWithContext(ctx).Info("no valid catalog fields to apply after validation",
			zap.String("destination_id", destID.String()),
			zap.String("source_id", sourceID.String()))
		return nil
	}

	// Build ALTER TABLE with fully-qualified table name
	database := databaseName(tenantID)
	var colDefs []string
	for _, f := range valid {
		colDefs = append(colDefs, fmt.Sprintf("%s %s", f.Name, athenaType(f.FieldType)))
	}
	query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMNS (%s)", database, tableName, strings.Join(colDefs, ", "))

	logger.GetLoggerWithContext(ctx).Info("executing Athena ALTER TABLE",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.String("region", region),
		zap.Int("columns", len(colDefs)),
		zap.String("query", query))

	if err := athena.RunDDLInRegion(ctx, query, region, outputLocation); err != nil {
		return fmt.Errorf("ALTER TABLE failed for %s.%s: %w", database, tableName, err)
	}

	logger.GetLoggerWithContext(ctx).Info("successfully applied catalog fields to Athena",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.String("region", region),
		zap.Int("fields_added", len(colDefs)))

	return nil
}
