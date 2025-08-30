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
	ID          uuid.UUID    `gorm:"type:uuid;primary_key"`
	EntityID    uuid.UUID    `gorm:"type:uuid"`
	TenantID    uuid.UUID    `gorm:"type:uuid"`
	Criticality string       `gorm:"type:varchar(255)"`
	Config      *AlertConfig `gorm:"type:json"`
}

func ReadEntityConfigs(db *gorm.DB, entityType, alertType string, tenantId uuid.UUID, sourceIds []uuid.UUID) ([]EntityAlertsConfig, error) {
	var sourceEntityConfigs []EntityAlertsConfig
	err := db.Where("tenant_id = ? AND entity_type = ? AND alert_type = ? AND entity_id IN ?", tenantId, entityType, alertType, sourceIds).
		Find(&sourceEntityConfigs).Error
	return sourceEntityConfigs, err
}
