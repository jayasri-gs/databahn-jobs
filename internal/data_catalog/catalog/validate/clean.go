package validate

import (
	"context"
	"fmt"

	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// CleanFields runs PartitionCatalogFields, deletes invalid rows, and logs results.
func CleanFields(
	ctx context.Context,
	destID, sourceID, tenantID uuid.UUID,
	dispenserType string,
	fields []model.Field,
) ([]model.Field, error) {
	return cleanCatalogFields(ctx, destID, sourceID, tenantID, dispenserType, fields)
}

var (
	cleanCatalogFields = defaultCleanCatalogFields
	pluckAppliedNames  = defaultPluckAppliedNames
	deleteInvalidRows  = defaultDeleteInvalidRows
)

func defaultCleanCatalogFields(
	ctx context.Context,
	destID, sourceID, tenantID uuid.UUID,
	dispenserType string,
	fields []model.Field,
) ([]model.Field, error) {
	existingApplied, err := pluckAppliedNames(ctx, destID, sourceID, tenantID, dispenserType, lowerFieldNames(fields))
	if err != nil {
		return nil, fmt.Errorf("failed to load applied catalog fields for %s/%s: %w", destID, sourceID, err)
	}

	valid, invalid := PartitionCatalogFields(fields, existingApplied)
	groupFields := model.GroupFields(destID, sourceID, tenantID, dispenserType)

	invalidNames := make([]string, 0, len(invalid))
	for _, inv := range invalid {
		invalidNames = append(invalidNames, inv.Field.Name)
	}
	logger.GetLoggerWithContext(ctx).Info("catalog fields validated",
		append(groupFields,
			zap.Int("valid_count", len(valid)),
			zap.Int("invalid_count", len(invalid)),
			zap.Strings("invalid_names", invalidNames),
		)...)

	if len(invalid) > 0 {
		invalidIDs := make([]int64, len(invalid))
		for i, inv := range invalid {
			invalidIDs[i] = inv.Field.ID
			logger.GetLoggerWithContext(ctx).Warn("deleting invalid catalog field",
				append(groupFields,
					zap.String("name", inv.Field.Name),
					zap.String("reason", inv.Reason),
					zap.Int64("id", inv.Field.ID),
				)...)
		}
		if err := deleteInvalidRows(ctx, invalidIDs); err != nil {
			return nil, fmt.Errorf("failed to delete invalid catalog fields: %w", err)
		}
	}

	if len(valid) == 0 {
		logger.GetLoggerWithContext(ctx).Info("no valid catalog fields to apply after validation", groupFields...)
	}
	return valid, nil
}

func defaultPluckAppliedNames(
	ctx context.Context,
	destID, sourceID, tenantID uuid.UUID,
	dispenserType string,
	incomingLower []string,
) ([]string, error) {
	var existingApplied []string
	err := config.GetDB().WithContext(ctx).
		Table("data_catalog").
		Where("applied_on_search = ? AND destination_id = ? AND source_id = ? AND tenant_id = ? AND dispenser_type = ? AND LOWER(name) IN ?",
			true, destID, sourceID, tenantID, dispenserType, incomingLower).
		Pluck("name", &existingApplied).Error
	return existingApplied, err
}

func defaultDeleteInvalidRows(ctx context.Context, invalidIDs []int64) error {
	return config.GetDB().WithContext(ctx).
		Table("data_catalog").
		Where("id IN ?", invalidIDs).
		Delete(nil).Error
}
