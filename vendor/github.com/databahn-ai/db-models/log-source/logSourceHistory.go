package log_source

import (
	"time"

	config "github.com/databahn-ai/db-models/log-source-config"
	uuid "github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type LogSourceHistory struct {
	ID                    uuid.UUID `gorm:"primaryKey;type:uuid;"`
	HistoryVersion        int       `gorm:"primaryKey"`
	ModelVersion          string    `gorm:"type:VARCHAR(8)"`
	CreatedAt             time.Time
	UpdatedAt             time.Time
	Name                  string `gorm:"type:VARCHAR(100)"`
	Description           string `gorm:"type:VARCHAR(512)"`
	Schedule              string `gorm:"type:VARCHAR(36)"`
	Device                string `gorm:"type:VARCHAR(30)"`
	Type                  string `gorm:"type:VARCHAR(30)"`
	Version               string `gorm:"type:VARCHAR(36)"`
	Vendor                string `gorm:"type:VARCHAR(30)"`
	Filter                string `gorm:"type:VARCHAR(70)"`
	Port                  int
	Protocol              string
	ConnectorID           uuid.UUID `gorm:"type:uuid"`
	EdgeId                uuid.UUID `gorm:"type:uuid"`
	TenantUUID            uuid.UUID `gorm:"primaryKey;type:uuid" json:"-"`
	DeletedAt             gorm.DeletedAt
	StatsLastUpdated      time.Time
	Status                int
	EventCollected        int64
	EventDelivered        datatypes.JSON
	Timezone              string
	Scope                 string `gorm:"type:VARCHAR(16)"`
	Configuration         datatypes.JSON
	CreatedBy             string `gorm:"type:VARCHAR(36)"`
	UpdatedBy             string `gorm:"type:VARCHAR(36)"`
	TimezoneNormalization string `gorm:"type:VARCHAR(16)"`
	TimestampOverride     bool
	LogSourceConfigs      datatypes.JSONType[[]config.LogSourceConfig]
	FetchMechanism        string `gorm:"type:VARCHAR(32)"`
}

func NewLogSourceHistory(logSource LogSource, conf []config.LogSourceConfig) *LogSourceHistory {
	h := LogSourceHistory{}
	h.ID = logSource.ID
	h.HistoryVersion = logSource.HistoryVersion
	h.ModelVersion = ModelV1
	h.CreatedAt = logSource.CreatedAt
	h.UpdatedAt = logSource.UpdatedAt
	h.Name = logSource.Name
	h.Description = logSource.Description
	h.Schedule = logSource.Schedule
	h.Device = logSource.Device
	h.Type = logSource.Type
	h.Version = logSource.Version
	h.Vendor = logSource.Vendor
	h.Filter = logSource.Filter
	h.Port = logSource.Port
	h.Protocol = logSource.Protocol
	h.ConnectorID = logSource.ConnectorID
	h.EdgeId = logSource.EdgeId
	h.TenantUUID = logSource.TenantUUID
	h.DeletedAt = logSource.DeletedAt
	h.StatsLastUpdated = logSource.StatsLastUpdated
	h.Status = logSource.Status
	h.EventCollected = logSource.EventCollected
	h.EventDelivered = logSource.EventDelivered
	h.Timezone = logSource.Timezone
	h.Scope = logSource.Scope
	h.Configuration = logSource.Configuration
	h.CreatedBy = logSource.CreatedBy.String()
	h.UpdatedBy = logSource.UpdatedBy.String()
	h.TimezoneNormalization = logSource.TimezoneNormalization
	h.TimestampOverride = logSource.TimestampOverride
	h.LogSourceConfigs = datatypes.JSONType[[]config.LogSourceConfig]{Data: conf}
	h.FetchMechanism = logSource.FetchMechanism
	return &h
}

func GetAllLogSourceHistoryByIdAndTenantId(db *gorm.DB, id, tenantId string) ([]LogSourceHistory, error) {
	var history []LogSourceHistory
	err := db.Where("id = ? AND tenant_uuid = ?", id, tenantId).Order("history_version DESC").Find(&history).Error
	return history, err
}

func GetLogSourceHistoryByIdAndTenantIdAndVersion(db *gorm.DB, ruleId, tenantId string, version int) (LogSourceHistory, error) {
	history := LogSourceHistory{}
	err := db.Where("ID = ? AND tenant_uuid = ? AND history_version = ?", ruleId, tenantId, version).First(&history).Error
	return history, err
}
func (h *LogSourceHistory) Save(db *gorm.DB) error {
	return db.Create(h).Error
}
