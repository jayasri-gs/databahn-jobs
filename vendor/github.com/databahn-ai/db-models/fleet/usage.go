package fleet

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type FleetNodeStats struct {
	NodeId             uuid.UUID `gorm:"primaryKey;type:uuid"`
	FleetId            uuid.UUID `gorm:"type:uuid" validate:"required"`
	TenantUUID         uuid.UUID `gorm:"type:uuid" validate:"required"`
	CustomerId         string    `gorm:"type:VARCHAR(36)" validate:"required"`
	DiskFree           uint64    `validate:"required"`
	DiskPath           string    `gorm:"type:VARCHAR(200)" validate:"required"`
	DiskUsed           uint64    `validate:"required"`
	DiskTotal          uint64    `validate:"required"`
	DiskUsedPercentage float64   `validate:"required"`
	CpuUsedPercent     float64   `validate:"required"`
	MemoryFree         uint64    `validate:"required"`
	MemoryUsed         uint64    `validate:"required"`
	MemoryTotal        uint64    `validate:"required"`
	MemoryUsedPercent  float64   `validate:"required"`
	LastReportedAt     time.Time `gorm:"autoUpdateTime"`
	FirstReportedAt    time.Time `gorm:"autoCreateTime"`
}

func (*FleetNodeStats) Migrate(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).AutoMigrate(&FleetNodeStats{})
}

func (ns *FleetNodeStats) GetUsageByNode(ctx context.Context, db *gorm.DB, tenantId string) error {
	return db.WithContext(ctx).First(&ns, "tenant_uuid = ? AND node_id = ?", tenantId, ns.NodeId).Error
}

func (ns *FleetNodeStats) RecordUsage(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(&ns).Error
}

func (ns *FleetNodeStats) UpdateUsage(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Where("node_id = ?", ns.NodeId).Save(&ns).Error
}

func IsUsageExists(ctx context.Context, db *gorm.DB, nodeId string) (bool, error) {
	var exists bool
	err := db.Model(&FleetNodeStats{}).Select("count(*) > 0").Where("node_id = ?", nodeId).Find(&exists).Error
	if err != nil {
		return false, err
	}
	return exists, nil
}
