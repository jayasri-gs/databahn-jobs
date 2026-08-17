package enrichment

import (
	"context"

	dbmodels "github.com/databahn-ai/db-models/enrichment"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// HasActiveEnrichment checks if a pipeline has any active enrichment configurations
func HasActiveEnrichment(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID, tenantID uuid.UUID) (bool, int64, error) {
	var count int64
	err := db.WithContext(ctx).Table("enrichment").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineID, tenantID, "ACTIVE").
		Count(&count).Error

	return count > 0, count, err
}

// GetActiveEnrichmentOutputFields returns mapping source_field names written by
// active enrichments on the pipeline (legacy db_enriched_N and custom names).
func GetActiveEnrichmentOutputFields(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID, tenantID uuid.UUID) (map[string]struct{}, error) {
	var enrichments []dbmodels.Enrichment
	err := db.WithContext(ctx).Table("enrichment").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineID, tenantID, "ACTIVE").
		Find(&enrichments).Error
	if err != nil {
		return nil, err
	}

	fields := make(map[string]struct{})
	for _, e := range enrichments {
		for _, name := range e.GetEnrichmentAttributes() {
			if name == "" {
				continue
			}
			fields[name] = struct{}{}
		}
	}
	return fields, nil
}
