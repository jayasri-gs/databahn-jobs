package fleet

import (
	"github.com/google/uuid"
	"time"
)

type Node struct {
	AlertLastReported time.Time `json:"alert_last_reported" gorm:"-"`
	BootTime          uint64    `json:"boot_time"`
	CpuArch           string    `json:"cpu_arch"`
	CpuCount          int       `json:"cpu_count"`
	CustomerId        uuid.UUID `json:"customer_id"`
	Description       string    `json:"description"`
	FleetId           uuid.UUID `json:"fleet_id"`
	HeartbeatAt       string    `json:"heartbeat_at"`
	Hostname          string    `json:"hostname"`
	Id                uuid.UUID `json:"id"`
	IsInitNode        bool      `json:"is_init_node"`
	KernelArch        string    `json:"kernel_arch"`
	KernelVersion     string    `json:"kernel_version"`
	Name              string    `json:"name"`
	Os                string    `json:"os"`
	Platform          string    `json:"platform"`
	PrivateIp         string    `json:"private_ip"`
	PublicIp          string    `json:"public_ip"`
	Role              string    `json:"role"`
	Status            string    `json:"status"`
	TenantId          uuid.UUID `json:"tenant_id"`
	Uptime            uint64    `json:"uptime"`
	CreatedAt         time.Time `json:"created_at"`
	CreatedBy         uuid.UUID `json:"created_by"`
	UpdatedAt         time.Time `json:"updated_at"`
	UpdatedBy         uuid.UUID `json:"updated_by"`
}

type HardwareStats struct {
	Id                 uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	CpuUsedPercent     float64   `gorm:"type:float8" json:"cpu_used_percent"`
	CustomerId         string    `gorm:"type:varchar(36)" json:"customer_id"`
	DiskFree           int64     `gorm:"type:bigint" json:"disk_free"`
	DiskPath           string    `gorm:"type:varchar(200)" json:"disk_path"`
	DiskTotal          int64     `gorm:"type:bigint" json:"disk_total"`
	DiskUsed           int64     `gorm:"type:bigint" json:"disk_used"`
	DiskUsedPercentage float64   `gorm:"type:float8" json:"disk_used_percentage"`
	FirstReportedAt    time.Time `gorm:"type:timestamp" json:"first_reported_at"`
	LastReportedAt     time.Time `gorm:"type:timestamp" json:"last_reported_at"`
	MemoryFree         int64     `gorm:"type:bigint" json:"memory_free"`
	MemoryTotal        int64     `gorm:"type:bigint" json:"memory_total"`
	MemoryUsed         int64     `gorm:"type:bigint" json:"memory_used"`
	MemoryUsedPercent  float64   `gorm:"type:float8" json:"memory_used_percent"`
	TenantId           uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	FleetId            uuid.UUID `gorm:"type:uuid;not null" gorm:"foreignkey:FleetId" json:"fleet_id"`
	NodeId             uuid.UUID `gorm:"type:uuid;not null" gorm:"foreignkey:NodeId" json:"node_id"`
}

func (s *HardwareStats) TableName() string {
	return "fleet_node_stats"
}

func (s *Node) TableName() string {
	return "fleet_node"
}
