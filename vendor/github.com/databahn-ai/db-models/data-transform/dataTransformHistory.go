package data_transform

import (
	data_transform_function "github.com/databahn-ai/db-models/data-transform-function"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"time"
)

type DataTransformHistory struct {
	ID                     uuid.UUID `gorm:"primarykey;type:uuid"`
	HistoryVersion         int       `gorm:"primaryKey"`
	ModelVersion           string    `gorm:"type:VARCHAR(8)"`
	TenantUUID             uuid.UUID `gorm:"primarykey;type:uuid" json:"-"`
	Name                   string    `gorm:"type:VARCHAR(100)"`
	Description            string    `gorm:"type:VARCHAR(512)"`
	SourceId               uuid.UUID `gorm:"type:uuid"`
	DestinationId          uuid.UUID `gorm:"type:uuid"`
	EventDependencyId      uuid.UUID `gorm:"type:uuid"`
	Status                 int
	CreatedBy              string `gorm:"type:VARCHAR(100)"`
	UpdatedBy              string `gorm:"type:VARCHAR(100)"`
	CreatedAt              time.Time
	UpdatedAt              time.Time
	DeletedAt              gorm.DeletedAt
	TransformationType     string `gorm:"type:VARCHAR(512)"`
	TransformationOutput   string `gorm:"type:VARCHAR(512)"`
	DataTransformFunctions datatypes.JSONType[data_transform_function.DataTransformFunction]
	ProcessedRawEvent      bool
}

func NewDataTransformHistory(transform DataTransform) *DataTransformHistory {
	d := DataTransformHistory{
		ID:                     transform.ID,
		HistoryVersion:         transform.HistoryVersion,
		ModelVersion:           ModelV1,
		TenantUUID:             transform.TenantUUID,
		Name:                   transform.Name,
		Description:            transform.Description,
		SourceId:               transform.SourceId,
		DestinationId:          transform.DestinationId,
		EventDependencyId:      transform.EventDependencyId,
		Status:                 transform.Status,
		CreatedBy:              transform.CreatedBy,
		UpdatedBy:              transform.UpdatedBy,
		CreatedAt:              transform.CreatedAt,
		UpdatedAt:              transform.UpdatedAt,
		DeletedAt:              transform.DeletedAt,
		TransformationType:     transform.TransformationType,
		TransformationOutput:   transform.TransformationOutput,
		DataTransformFunctions: datatypes.JSONType[data_transform_function.DataTransformFunction]{Data: transform.DataTransformFunctions},
		ProcessedRawEvent:      transform.ProcessedRawEvent,
	}
	return &d
}

func GetAllDataTransformHistoryByIdAndTenantId(db *gorm.DB, id, tenantId string) ([]DataTransformHistory, error) {
	var history []DataTransformHistory
	err := db.Where("id = ? AND tenant_uuid = ?", id, tenantId).Order("history_version DESC").Find(&history).Error
	return history, err
}

func (d *DataTransformHistory) Save(db *gorm.DB) error {
	return db.Create(d).Error
}
