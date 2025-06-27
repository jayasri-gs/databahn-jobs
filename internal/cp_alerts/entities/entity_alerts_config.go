package entities

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type Interval string

const Minute Interval = "MINUTE"
const Hour Interval = "HOUR"
const Day Interval = "DAY"
const Week Interval = "WEEK"

type AlertConfig struct {
	Enabled                        bool                            `json:"enabled"`
	LogSourceInactivityAlertConfig *LogSourceInactivityAlertConfig `json:"logSourceInactivityAlertConfig"`
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

func ReadSourceEntityConfigs(db *gorm.DB, alertType string, tenantId uuid.UUID, sourceIds []uuid.UUID) ([]EntityAlertsConfig, error) {
	var sourceEntityConfigs []EntityAlertsConfig
	err := db.Where("tenant_id = ? AND entity_type = 'LOG_SOURCE' AND alert_type = ? AND entity_id IN ?", tenantId, alertType, sourceIds).
		Find(&sourceEntityConfigs).Error
	return sourceEntityConfigs, err
}
