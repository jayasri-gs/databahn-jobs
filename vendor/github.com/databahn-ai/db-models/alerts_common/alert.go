package alerts_common

import (
	"time"

	"github.com/google/uuid"
)

// Deprecated: This is deprecated and will be removed soon.
type Alert struct {
	ID                      uuid.UUID `gorm:"primaryKey;type:uuid" json:"id"`
	Criticality             string    `gorm:"not null" validate:"required" json:"criticality"`
	Title                   string    `gorm:"not null;type:VARCHAR(128)" validate:"required" json:"title"`
	Message                 string    `gorm:"type:VARCHAR(512)" json:"message"`
	CreatedAt               time.Time `json:"createdAt,omitempty"`
	UpdatedAt               time.Time `json:"updatedAt,omitempty"`
	FirstObservedAt         time.Time `json:"firstObservedAt,omitempty"`
	LastObservedAt          time.Time `json:"lastObservedAt,omitempty"`
	TenantUUID              uuid.UUID `gorm:"type:uuid" validate:"required" json:"tenantId"`
	Functionality           string    `gorm:"not null;type:VARCHAR(64)" validate:"required" json:"functionality"`
	FunctionalityEntityId   string    `gorm:"type:VARCHAR(36)" json:"functionalityEntityId"`
	FunctionalityEntityName string    `gorm:"type:VARCHAR(128)" json:"functionalityEntityName"`
	FunctionalityType       string    `gorm:"type:VARCHAR(64)" json:"functionalityType"`
	Status                  int       `json:"status"`
	UpdatedBy               string    `gorm:"type:VARCHAR(100)" validate:"required"`
	Dismissed               bool      `json:"dismissed"`
	AlertType               string    `gorm:"type:VARCHAR(64)" json:"alertType"`
	ErrorMessage            string    `gorm:"type:text" json:"errorMessage"`
	ErrorCode               string    `gorm:"type:VARCHAR(128)" json:"errorCode"`
	DataPlaneId             uuid.UUID `gorm:"type:uuid" validate:"required" json:"dataPlaneId"`
}
