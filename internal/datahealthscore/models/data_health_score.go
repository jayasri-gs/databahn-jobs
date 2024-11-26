package models

import (
	"github.com/google/uuid"
	"time"
)

type DataHealthScore struct {
	ID          uuid.UUID `gorm:"type:uuid" json:"id"`
	TenantID    uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	EntityType  string    `gorm:"type:varchar(255)" json:"entity_type"`
	SourceID    uuid.UUID `gorm:"type:uuid" json:"source_id"`
	HealthScore float32   `gorm:"type:float" json:"health_score"`
	CreatedAt   time.Time `gorm:"type:timestamp" json:"created_at"`
	UpdatedAt   time.Time `gorm:"type:timestamp" json:"updated_at"`
}
