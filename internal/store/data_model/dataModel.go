package data_model

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// DataModel represents the data model entity
type DataModel struct {
	ID               uuid.UUID       `gorm:"type:uuid;primaryKey" json:"id"`
	Vendor           string          `gorm:"type:varchar(255)" json:"vendor"`
	Device           string          `gorm:"type:varchar(255)" json:"device"`
	LogType          string          `gorm:"type:varchar(255)" json:"log_type"`
	ApplicationType  string          `gorm:"type:varchar(255)" json:"application_type"`
	SampleValue      string          `gorm:"type:text" json:"sample_value"`
	LogAttribute     string          `gorm:"type:varchar(255)" json:"log_attribute"`
	DisplayName      string          `gorm:"type:varchar(255)" json:"display_name"`
	Name             string          `gorm:"type:varchar(255)" json:"name"`
	DataType         string          `gorm:"type:varchar(100)" json:"data_type"`
	AdditionalConfig json.RawMessage `gorm:"type:json" json:"additional_config"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// TableName returns the table name for DataModel
func (dm *DataModel) TableName() string {
	return "data_model"
}

// GetDataModelByDeviceVendorLogType fetches data models filtered by device, vendor, and log type
func GetDataModelByDeviceVendorLogType(ctx context.Context, db *gorm.DB, device, vendor, logType string) ([]DataModel, error) {
	var dataModels []DataModel

	err := db.WithContext(ctx).
		Where("device = ? AND vendor = ? AND log_type = ?", device, vendor, logType).
		Find(&dataModels).Error

	if err != nil {
		return nil, err
	}

	return dataModels, nil
}
