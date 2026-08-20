package apply

import (
	"context"
	"fmt"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/data_catalog/catalog/model"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const searchEngineAthena = "ATHENA"

// PartitionByPresence splits fields into those already present in Athena and those missing.
func PartitionByPresence(fields []model.Field, existing map[string]struct{}) (alreadyPresent, missing []model.Field) {
	for _, f := range fields {
		if _, ok := existing[strings.ToLower(f.Name)]; ok {
			alreadyPresent = append(alreadyPresent, f)
		} else {
			missing = append(missing, f)
		}
	}
	return alreadyPresent, missing
}

// MarkApplied sets applied_on_search for the given catalog row IDs.
func MarkApplied(ctx context.Context, ids []int64, phase string) error {
	if len(ids) == 0 {
		return nil
	}
	return markCatalogApplied(ctx, ids, phase)
}

var (
	markCatalogApplied     = defaultMarkCatalogApplied
	updateCatalogAppliedRows = defaultUpdateCatalogAppliedRows
)

func defaultMarkCatalogApplied(ctx context.Context, ids []int64, phase string) error {
	err := updateCatalogAppliedRows(ctx, ids)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("failed to mark catalog fields as applied",
			zap.String("phase", phase),
			zap.Int64s("catalog_ids", ids),
			zap.Error(err))
		return fmt.Errorf("failed to update applied_on_search (%s): %w", phase, err)
	}
	return nil
}

func defaultUpdateCatalogAppliedRows(ctx context.Context, ids []int64) error {
	return config.GetDB().WithContext(ctx).
		Table("data_catalog").
		Where("id IN ?", ids).
		Updates(map[string]interface{}{
			"applied_on_search": true,
			"search_engine":     searchEngineAthena,
		}).Error
}
