package models

import (
	"github.com/google/uuid"
	"time"
)

type DataHealthScoreRecord struct {
	ID                  uuid.UUID              `json:"id"`
	DataHealthScoreID   uuid.UUID              `gorm:"type:uuid" json:"data_health_score_id"`
	TenantID            uuid.UUID              `gorm:"type:uuid" json:"tenant_id"`
	CreatedAt           time.Time              `gorm:"type:timestamp" json:"created_at"`
	ViolationType       string                 `gorm:"type:varchar(255)" json:"violation_type"`
	ViolationSubtype    string                 `gorm:"type:varchar(255)" json:"violation_subtype"`
	Message             string                 `gorm:"type:text" json:"message"`
	PercentageReduction float32                `gorm:"type:float" json:"percentage_reduction"`
	ResolutionStatus    string                 `gorm:"type:varchar(255)" json:"resolution_status"`
	Details             map[string]interface{} `gorm:"-" json:"details"`
}
