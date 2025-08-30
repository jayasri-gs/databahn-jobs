package source

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Configuration struct {
	Configuration map[string]interface{} `json:"configuration"`
}

type AdvancedConfiguration struct {
	SendUnmatchedEventToPrimaryDestination bool `json:"sendUnmatchedEventToPrimaryDestination"`
}

type Source struct {
	ID                           uuid.UUID              `gorm:"type:uuid;primary_key" json:"id"`
	Configuration                datatypes.JSON         `json:"configuration"`
	ConnectorID                  uuid.UUID              `gorm:"type:uuid" json:"connector_id"`
	CreatedAt                    time.Time              `gorm:"type:timestamp" json:"created_at"`
	CreatedBy                    uuid.UUID              `gorm:"type:uuid" json:"created_by"`
	CustomerID                   uuid.UUID              `gorm:"type:uuid" json:"customer_id"`
	Description                  string                 `gorm:"type:varchar(512)" json:"description"`
	Device                       string                 `gorm:"type:varchar(30)" json:"device"`
	FleetID                      uuid.UUID              `gorm:"type:uuid" json:"fleet_id"`
	LogType                      string                 `gorm:"type:varchar(30)" json:"log_type"`
	Name                         string                 `gorm:"type:varchar(100)" json:"name"`
	ReplaySource                 bool                   `json:"replay_source"`
	Reputation                   string                 `gorm:"type:varchar(255)" json:"reputation"`
	Scope                        string                 `gorm:"type:varchar(255)" json:"scope"`
	Status                       string                 `gorm:"type:varchar(255)" json:"status"`
	TenantID                     uuid.UUID              `gorm:"type:uuid" json:"tenant_id"`
	TimestampOverrideEnabled     bool                   `json:"timestamp_override_enabled"`
	TimezoneNormalizationEnabled bool                   `json:"timezone_normalization_enabled"`
	UpdatedAt                    time.Time              `gorm:"type:timestamp" json:"updated_at"`
	UpdatedBy                    uuid.UUID              `gorm:"type:uuid" json:"updated_by"`
	Vendor                       string                 `gorm:"type:varchar(30)" json:"vendor"`
	Version                      string                 `gorm:"type:varchar(36)" json:"version"`
	Config                       map[string]interface{} `gorm:"-" json:"-"`
	DataPlaneId                  uuid.UUID              `gorm:"type:uuid" json:"data_plane_id"`
	AdvancedConfiguration        AdvancedConfiguration  `gorm:"column:advanced_configuration" json:"advanced_configuration"`
}

func (s *Source) TableName() string {
	return "log_source"
}

// GetSourcesByTenantAndStatus gets all log sources for a tenant with the given status
func GetSourcesByTenantAndStatus(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, status string) ([]Source, error) {
	var sources []Source
	err := db.WithContext(ctx).
		Where("tenant_id = ? AND status = ?", tenantId, status).
		Find(&sources).Error
	if err != nil {
		return nil, fmt.Errorf("error getting sources for tenant %s with status %s: %w", tenantId.String(), status, err)
	}
	return sources, nil
}
