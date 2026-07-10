package fixtures

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// InsightsFixture holds tenant and log source IDs for insights aggregation tests.
type InsightsFixture struct {
	TenantID    uuid.UUID
	SourceID    uuid.UUID
	DataPlaneID uuid.UUID
}

// SeedInsightsFixture creates an isolated tenant and log source for insights aggregation tests.
func SeedInsightsFixture(ctx context.Context, db *gorm.DB) (*InsightsFixture, error) {
	tenantID := uuid.New()
	sourceID := uuid.New()
	dataPlaneID := uuid.New()
	actorID := uuid.New()
	now := time.Now().UTC()

	tenant := map[string]any{
		"id":     tenantID,
		"name":   fmt.Sprintf("insights-integration-%s", tenantID.String()[:8]),
		"active": true,
	}
	if err := db.WithContext(ctx).Table("tenants").
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoNothing: true}).
		Create(tenant).Error; err != nil {
		return nil, fmt.Errorf("insert insights tenant: %w", err)
	}

	source := map[string]any{
		"id":            sourceID,
		"name":          "insights-integration-source",
		"tenant_id":     tenantID,
		"data_plane_id": dataPlaneID,
		"status":        "ACTIVE",
		"created_by":    actorID,
		"updated_by":    actorID,
		"customer_id":   tenantID,
		"created_at":    now,
		"updated_at":    now,
	}
	if err := db.WithContext(ctx).Table("log_source").Create(source).Error; err != nil {
		return nil, fmt.Errorf("insert insights log source: %w", err)
	}

	return &InsightsFixture{
		TenantID:    tenantID,
		SourceID:    sourceID,
		DataPlaneID: dataPlaneID,
	}, nil
}
