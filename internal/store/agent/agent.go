package agent

import (
	"github.com/google/uuid"
	"time"
)

type Agent struct {
	ID          uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	HeartbeatAt time.Time `gorm:"type:timestamp" json:"heartbeat_at"`
	Hostname    string    `gorm:"type:varchar(100)" json:"hostname"`
	Name        string    `gorm:"type:varchar(255)" json:"name"`
	Os          string    `gorm:"type:varchar(100)" json:"os"`
	Platform    string    `gorm:"type:varchar(100)" json:"platform"`
	TenantId    uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
}

func (s *Agent) TableName() string {
	return "agent_node"
}
