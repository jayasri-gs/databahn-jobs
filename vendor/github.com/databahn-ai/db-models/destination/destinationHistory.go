package destination

import (
	"time"

	destination_config "github.com/databahn-ai/db-models/destination-config"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type DestinationHistory struct {
	ID                 uuid.UUID `gorm:"primarykey;type:uuid"`
	HistoryVersion     int       `gorm:"primaryKey"`
	ModelVersion       string    `gorm:"type:VARCHAR(8)"`
	Name               string    `gorm:"type:VARCHAR(100)"`
	Description        string    `gorm:"type:VARCHAR(512)"`
	TenantUUID         uuid.UUID `gorm:"primarykey;type:uuid" json:"-"`
	DestinationType    string    `gorm:"type:VARCHAR(36)"`
	CreatedBy          string    `gorm:"type:VARCHAR(100)"`
	UpdatedBy          string    `gorm:"type:VARCHAR(100)"`
	Enabled            bool
	ForwardDataType    int
	Status             int
	CreatedAt          time.Time
	UpdatedAt          time.Time
	HeartBeatAt        time.Time
	DeletedAt          gorm.DeletedAt `gorm:"index"`
	Scope              string         `gorm:"type:VARCHAR(16)"`
	EntityId           string         `gorm:"type:VARCHAR(36)"`
	DestinationConfigs datatypes.JSONType[[]destination_config.DestinationConfig]
}

func NewDestinationHistory(dest Destination, conf []destination_config.DestinationConfig) *DestinationHistory {
	h := DestinationHistory{}
	h.ID = dest.ID
	h.HistoryVersion = dest.HistoryVersion
	h.ModelVersion = ModelV1
	h.Name = dest.Name
	h.Description = dest.Description
	h.TenantUUID = dest.TenantUUID
	h.DestinationType = dest.DestinationType
	h.CreatedBy = dest.CreatedBy
	h.Enabled = dest.Enabled
	h.ForwardDataType = dest.ForwardDataType
	h.CreatedAt = dest.CreatedAt
	h.UpdatedAt = dest.UpdatedAt
	h.DeletedAt = dest.DeletedAt
	h.UpdatedBy = dest.UpdatedBy
	h.Scope = dest.Scope
	h.EntityId = dest.EntityId
	h.DestinationConfigs = datatypes.NewJSONType(conf)
	return &h
}

func GetAllLogSourceHistoryByIdAndTenantId(db *gorm.DB, id, tenantId string) ([]DestinationHistory, error) {
	var history []DestinationHistory
	err := db.Where("id = ? AND tenant_uuid = ?", id, tenantId).Order("history_version DESC").Find(&history).Error
	return history, err
}

func GetLogSourceHistoryByIdAndTenantIdAndVersion(db *gorm.DB, ruleId, tenantId string, version int) (DestinationHistory, error) {
	history := DestinationHistory{}
	err := db.Where("ID = ? AND tenant_uuid = ? AND history_version = ?", ruleId, tenantId, version).First(&history).Error
	return history, err
}
func (h *DestinationHistory) Save(db *gorm.DB) error {
	return db.Create(h).Error
}
