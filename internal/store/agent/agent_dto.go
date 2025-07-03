package agent

import (
	"time"

	"github.com/google/uuid"
)

type Agent struct {
	ID                 uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	BootTime           int64     `gorm:"type:bigint" json:"boot_time"`
	CpuArch            string    `gorm:"type:varchar(255)" json:"cpu_arch"`
	CpuCount           int       `gorm:"type:int" json:"cpu_count"`
	CreatedAt          time.Time `gorm:"type:timestamp" json:"created_at"`
	CreatedBy          uuid.UUID `gorm:"type:uuid" json:"created_by"`
	CustomerId         uuid.UUID `gorm:"type:uuid" json:"customer_id"`
	Description        string    `gorm:"type:varchar(255)" json:"description"`
	HeartbeatAt        time.Time `gorm:"type:timestamp" json:"heartbeat_at"`
	Hostname           string    `gorm:"type:varchar(100)" json:"hostname"`
	IsUpgradeAvailable bool      `gorm:"default:false" json:"is_upgrade_available"`
	KernelArch         string    `gorm:"type:varchar(100)" json:"kernel_arch"`
	KernelVersion      string    `gorm:"type:varchar(100)" json:"kernel_version"`
	Name               string    `gorm:"type:varchar(255)" json:"name"`
	Os                 string    `gorm:"type:varchar(100)" json:"os"`
	Platform           string    `gorm:"type:varchar(100)" json:"platform"`
	PrivateIp          string    `gorm:"type:varchar(20)" json:"private_ip"`
	PublicIp           string    `gorm:"type:varchar(20)" json:"public_ip"`
	Status             string    `gorm:"type:varchar(255)" json:"status"`
	TenantId           uuid.UUID `gorm:"type:uuid" json:"tenant_id"`
	UpdatedAt          time.Time `gorm:"type:timestamp" json:"updated_at"`
	UpdatedBy          uuid.UUID `gorm:"type:uuid" json:"updated_by"`
	Uptime             int64     `gorm:"type:bigint" json:"uptime"`
	FleetId            uuid.UUID `gorm:"type:uuid;not null" json:"fleet_id"`
	LogsourceId        uuid.UUID `gorm:"type:uuid" json:"logsource_id"`
	DataPlaneId        uuid.UUID `gorm:"type:uuid" json:"data_plane_id"`
	Port               int       `gorm:"type:int" json:"port"`
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
	FleetId            uuid.UUID `gorm:"type:uuid;not null;foreignkey:FleetId" json:"fleet_id"`
	NodeId             uuid.UUID `gorm:"type:uuid;not null;foreignkey:NodeId" json:"node_id"`
}

type Config struct {
	Config  []byte
	Leaders []string
	Agent   *Agent
}

func (*HardwareStats) TableName() string {
	return "fleet_node_stats"
}

func (*Agent) TableName() string {
	return "agent_node"
}
