package enrichment

import (
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// HasActiveEnrichment checks if a pipeline has any active enrichment configurations
func HasActiveEnrichment(ctx context.Context, db *gorm.DB, pipelineID uuid.UUID, tenantID uuid.UUID) (bool, int64, error) {
	var count int64
	err := db.WithContext(ctx).Table("enrichment").
		Where("pipeline_id = ? AND tenant_id = ? AND status = ?",
			pipelineID.String(), tenantID.String(), "ACTIVE").
		Count(&count).Error

	return count > 0, count, err
}
