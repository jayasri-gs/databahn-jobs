package model

import (
	"github.com/google/uuid"
	"time"
)

type Device struct {
	Hostname         string `json:"hostname"`
	MinTime          int64  `json:"min_time"`
	MaxTime          int64  `json:"max_time"`
	MinTimeFormatted string `json:"min_time_formatted,omitempty"`
	MaxTimeFormatted string `json:"max_time_formatted,omitempty"`
	SourceID         string `json:"source_id"`
	TenantId         string `json:"tenant_id"`
	TenantName       string `json:"tenant_name"`
	SourceName       string `json:"source_name"`
	Summary          string `json:"summary,omitempty"`
}

type SilentDevicesConfig struct {
	ID         uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	SourceId   uuid.UUID `gorm:"type:uuid" json:"source_id"`
	TenantID   uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	CustomerID uuid.UUID `gorm:"type:uuid" json:"customer_id"`
	CreatedAt  time.Time `gorm:"type:timestamp" json:"created_at"`
	UpdatedAt  time.Time `gorm:"-" json:"updated_at"`
}
