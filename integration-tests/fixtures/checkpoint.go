package fixtures

import (
	"context"
	"fmt"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// SeedExternalAlertCheckpoint inserts a notification checkpoint for external alerts.
func SeedExternalAlertCheckpoint(ctx context.Context, db *gorm.DB, tenantID uuid.UUID, lastObservedAt int64) error {
	cp := entities.AlertNotificationCheckpoint{
		Id:       uuid.New(),
		TenantId: tenantID,
		CheckpointValue: &entities.CheckpointValue{
			LastObservedAt: lastObservedAt,
		},
		AlertType: alerts_async.External.String(),
	}
	if err := db.WithContext(ctx).Create(&cp).Error; err != nil {
		return fmt.Errorf("insert external alert checkpoint: %w", err)
	}
	return nil
}
