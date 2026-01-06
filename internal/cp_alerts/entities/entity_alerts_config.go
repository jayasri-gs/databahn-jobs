package entities

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Interval string

const Minute Interval = "MINUTE"
const Hour Interval = "HOUR"
const Day Interval = "DAY"
const Week Interval = "WEEK"

type IncludeExclude string

const Include IncludeExclude = "INCLUDE"
const Exclude IncludeExclude = "EXCLUDE"

const LogSourceEntityType = "LOG_SOURCE"
const DestinationEntityType = "DESTINATION"
const TenantEntityType = "TENANT"

type Reputation string

const (
	ReputationSilent     Reputation = "SILENT"
	ReputationStable     Reputation = "STABLE"
	ReputationWhispering Reputation = "WHISPERING"
	ReputationNoisy      Reputation = "NOISY"
)

type AlertConfig struct {
	Enabled                             bool                                 `json:"enabled"`
	LogSourceInactivityAlertConfig      *LogSourceInactivityAlertConfig      `json:"logSourceInactivityAlertConfig"`
	DestinationInactivityAlertConfig    *DestinationInactivityAlertConfig    `json:"destinationInactivityAlertConfig"`
	LogSourceDeviceInventoryAlertConfig *LogSourceDeviceInventoryAlertConfig `json:"logSourceDeviceInventoryAlertConfig"`
	DestinationDeliveredMoreAlertConfig *DestinationDeliveredMoreAlertConfig `json:"destinationDeliveredMoreAlertConfig"`
	VolumeDeviationAlertConfig          *VolumeDeviationAlertConfig          `json:"volumeDeviationAlertConfig"`
	DisableSandboxAlertsConfig          *DisableSandboxAlertsConfig          `json:"disableSandboxAlertsConfig"`
	DropRuleIncreaseAlertConfig         *DropRuleIncreaseAlertConfig         `json:"dropRuleIncreaseAlertConfig"`
}

func (a *AlertConfig) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to scan AlertConfig: not []byte")
	}
	return json.Unmarshal(bytes, a)
}

func (a *AlertConfig) Value() (driver.Value, error) {
	return json.Marshal(a)
}

type LogSourceInactivityAlertConfig struct {
	InactivityDuration *Duration `json:"inactivityDuration"`
}

type DestinationInactivityAlertConfig struct {
	InactivityDuration *Duration `json:"inactivityDuration"`
}

type LogSourceDeviceInventoryAlertConfig struct {
	ReputationsToAlert []Reputation   `json:"reputationsToAlert"`
	VcRuleFilters      *VcRuleFilter  `json:"vcRuleFilters"`
	IncludeExclude     IncludeExclude `json:"includeExclude"`
}

type DestinationDeliveredMoreAlertConfig struct {
	DifferencePercentageThreshold   int    `json:"differencePercentageThreshold"`
	MinimumIngestionVolumeThreshold int64  `json:"minimumIngestionVolumeThreshold"`
	MinimumIngestionVolumeUnit      string `json:"minimumIngestionVolumeUnit"`
}

type VolumeDeviationAlertConfig struct {
	DeviationPercentage        int    `json:"deviationPercentage"`
	MinimumVolumeThreshold     int64  `json:"minimumVolumeThreshold"`
	MinimumVolumeThresholdUnit string `json:"minimumVolumeThresholdUnit"`
}

type DropRuleIncreaseAlertConfig struct {
	DropPercentage      int   `json:"dropPercentage"`
	MinimumEventMatched int64 `json:"minimumEventMatched"`
}

type DisableSandboxAlertsConfig struct {
	// This config uses the AlertConfig.Enabled field to control whether sandbox alerts are disabled
	// No additional fields are needed
}

type VcRuleFilter struct {
	Field      string         `json:"field"`
	Value      string         `json:"value"`
	Operator   string         `json:"operator"`
	Rules      []VcRuleFilter `json:"rules"`
	Combinator string         `json:"combinator"`
}

type Duration struct {
	Time     int      `json:"time"`
	Interval Interval `json:"interval"`
}

func (d *Duration) GetDuration() (time.Duration, error) {
	switch d.Interval {
	case Minute:
		return time.Duration(d.Time) * time.Minute, nil
	case Hour:
		return time.Duration(d.Time) * time.Hour, nil
	case Day:
		return time.Duration(d.Time) * time.Hour * 24, nil
	case Week:
		return time.Duration(d.Time) * time.Hour * 24 * 7, nil
	default:
		return 0, fmt.Errorf("invalid interval: %s", d.Interval)
	}
}

type EntityAlertsConfig struct {
	ID         uuid.UUID    `gorm:"type:uuid;primary_key;column:id"`
	EntityID   uuid.UUID    `gorm:"type:uuid;column:entity_id"`
	EntityType string       `gorm:"type:varchar(255);column:entity_type"`
	AlertType  string       `gorm:"type:varchar(255);column:alert_type"`
	TenantID   uuid.UUID    `gorm:"type:uuid;column:tenant_id"`
	CustomerID uuid.UUID    `gorm:"type:uuid;column:customer_id"`
	Config     *AlertConfig `gorm:"type:json;column:config"`
	CreatedBy  uuid.UUID    `gorm:"type:uuid;column:created_by"`
	UpdatedBy  uuid.UUID    `gorm:"type:uuid;column:updated_by"`
	CreatedAt  time.Time    `gorm:"column:created_at"`
	UpdatedAt  time.Time    `gorm:"column:updated_at"`
}

// TableName specifies the table name for GORM
func (EntityAlertsConfig) TableName() string {
	return "entity_alerts_config"
}

func ReadEntityConfigs(db *gorm.DB, entityType, alertType string, tenantId uuid.UUID, entityIds []uuid.UUID) ([]EntityAlertsConfig, error) {
	var entityConfigs []EntityAlertsConfig
	err := db.Where("tenant_id = ? AND entity_type = ? AND alert_type = ? AND entity_id IN ?", tenantId, entityType, alertType, entityIds).
		Find(&entityConfigs).Error
	return entityConfigs, err
}

// ReadTenantLevelConfigs reads the latest tenant-level alert configuration based on updated_at timestamp
func ReadTenantLevelConfigs(db *gorm.DB, alertType string, tenantId uuid.UUID) ([]EntityAlertsConfig, error) {
	var tenantConfigs []EntityAlertsConfig
	err := db.Where("tenant_id = ? AND entity_type = ? AND alert_type = ?", tenantId, TenantEntityType, alertType).
		Order("updated_at DESC").
		Limit(1).
		Find(&tenantConfigs).Error
	return tenantConfigs, err
}
