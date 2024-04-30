package source

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"time"
)

type Configuration struct {
	Configuration map[string]interface{} `json:"configuration"`
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
}

func (s *Source) TableName() string {
	return "log_source"
}
