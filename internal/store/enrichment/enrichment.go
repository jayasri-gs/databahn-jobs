package enrichment

import (
	"context"

	dbmodels "github.com/databahn-ai/db-models/enrichment"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// GetActiveEnrichments returns active enrichment rows for a pipeline.
func GetActiveEnrichments(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID, tenantID uuid.UUID) ([]dbmodels.Enrichment, error) {
	var enrichments []dbmodels.Enrichment
	err := db.WithContext(ctx).Table("enrichment").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineID, tenantID, "ACTIVE").
		Find(&enrichments).Error
	return enrichments, err
}

// HasActiveEnrichment checks if a pipeline has any active enrichment configurations
func HasActiveEnrichment(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID, tenantID uuid.UUID) (bool, int64, error) {
	var count int64
	err := db.WithContext(ctx).Table("enrichment").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineID, tenantID, "ACTIVE").
		Count(&count).Error

	return count > 0, count, err
}

// OutputFieldsFromEnrichments returns mapping source_field names written by
// enrichments (legacy db_enriched_N and custom names).
func OutputFieldsFromEnrichments(enrichments []dbmodels.Enrichment) map[string]struct{} {
	fields := make(map[string]struct{})
	for _, e := range enrichments {
		for _, name := range e.GetEnrichmentAttributes() {
			if name == "" {
				continue
			}
			fields[name] = struct{}{}
		}
	}
	return fields
}
