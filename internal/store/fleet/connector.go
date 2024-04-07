package fleet

import (
	"github.com/google/uuid"
	"time"
)

type Connector struct {
	ID                 uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	Active             bool      `gorm:"default:true" json:"active"`
	CreatedAt          time.Time `gorm:"type:timestamp" json:"created_at"`
	CreatedBy          uuid.UUID `gorm:"type:uuid" json:"created_by"`
	Description        string    `gorm:"type:varchar(255)" json:"description"`
	FleetID            uuid.UUID `gorm:"type:uuid" json:"fleet_id"`
	HeartbeatAt        time.Time `gorm:"type:heartbeat_at" json:"heartbeat_at"`
	IsUpgradeAvailable bool      `json:"is_upgrade_available"`
	LastUpgraded       time.Time `gorm:"type:timestamp" json:"last_upgraded"`
	Name               string    `gorm:"type:varchar(255);not null" json:"name"`
	Status             string    `gorm:"type:varchar(255)" json:"status"`
	TenantID           uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	CustomerID         uuid.UUID `gorm:"type:uuid" json:"customer_id"`
	Type               string    `gorm:"type:varchar(255)" json:"type"`
	UpdatedAt          time.Time `gorm:"type:timestamp" json:"updated_at"`
	Version            string    `gorm:"type:varchar(255)" json:"version"`
}

func (c *Connector) TableName() string {
	return "connector"
}
