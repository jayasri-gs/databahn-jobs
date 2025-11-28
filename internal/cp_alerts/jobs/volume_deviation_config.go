package jobs

import (
	"fmt"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// getVolumeDeviationConfigForTenant fetches the latest tenant-level configuration for volume deviation alerts
func getVolumeDeviationConfigForTenant(db *gorm.DB, tenantId uuid.UUID) (*entities.EntityAlertsConfig, error) {
	configs, err := entities.ReadTenantLevelConfigs(db, "VOLUME_DEVIATION", tenantId)
	if err != nil {
		return nil, fmt.Errorf("failed to read tenant-level configs: %w", err)
	}

	if len(configs) == 0 {
		return nil, nil
	}

	// Return the first config (ordered by updated_at DESC, so this is the most recent)
	return &configs[0], nil
}
