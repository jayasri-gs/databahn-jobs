package entities

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type CheckpointValue struct {
	LastObservedAt int64 `json:"last_observed_at"`
}

func (a *CheckpointValue) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to scan CheckpointValue: not []byte")
	}
	return json.Unmarshal(bytes, a)
}

func (a *CheckpointValue) Value() (driver.Value, error) {
	return json.Marshal(a)
}

type AlertNotificationCheckpoint struct {
	Id              uuid.UUID        `gorm:"type:uuid;default:uuid_generate_v4();primary_key;column:id"`
	TenantId        uuid.UUID        `gorm:"type:uuid;not null;column:tenant_id"`
	CheckpointValue *CheckpointValue `gorm:"type:jsonb;not null;column:checkpoint_value"`
	AlertType       string           `gorm:"type:varchar(32);not null;column:alert_type"`
}

func GetAlertNotificationCheckpoint(db *gorm.DB, tenantId uuid.UUID, alertType alerts_async.AlertType) (*AlertNotificationCheckpoint, error) {
	cp := &AlertNotificationCheckpoint{}
	err := db.Where("tenant_id = ? AND alert_type = ?", tenantId, alertType.String()).First(cp).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cp, nil
}

func UpdateAlertNotificationCheckpoint(db *gorm.DB, cp *AlertNotificationCheckpoint) error {
	err := db.Save(cp).Error
	if err != nil {
		return err
	}
	return nil
}
