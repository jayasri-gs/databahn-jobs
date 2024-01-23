package fleet

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type FleetActivity struct {
	ID              uuid.UUID `gorm:"primaryKey" validate:"required"`
	FleetId         string    `json:"fleet_id" validate:"required"`
	NodeId          string    `gorm:"node_id" validate:"required"`
	EntityId        string    `gorm:"entity_id" validate:"required"`
	LastReportedAt  time.Time `json:"last_reported_at"`
	FirstReportedAt time.Time `json:"first_reported_at"`
	ActivityType    string    `json:"activity_type" validate:"required"`
	Description     string    `json:"description" validate:"required" `
	TenantId        uuid.UUID `json:"-" validate:"required"`
	DeletedAt       time.Time `json:"deleted_at,omitempty"`
}

func (a *FleetActivity) RecordActivity(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) error {
	a.ID = uuid.New()
	a.FirstReportedAt = time.Now()
	a.LastReportedAt = time.Now()
	a.TenantId = tenantId
	return db.WithContext(ctx).Create(&a).Error
}

func (a *FleetActivity) UpdateActivity(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) error {
	a.LastReportedAt = time.Now()
	a.TenantId = tenantId
	return db.WithContext(ctx).Where("id = ? AND tenant_id = ?", a.ID, a.TenantId).Updates(&a).Error
}

func (a *FleetActivity) DeleteActivity(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) error {
	a.LastReportedAt = time.Now()
	return db.WithContext(ctx).Delete(&a, "id = ? AND tenant_id = ?", a.ID, tenantId).Error
}

func GetActivitiesForFleet(ctx context.Context, db *gorm.DB, fleetId uuid.UUID, tenantId uuid.UUID) (a []FleetActivity, err error) {
	err = db.WithContext(ctx).Find(&a, "fleet_id = ? AND tenant_id = ?", fleetId, tenantId).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return []FleetActivity{}, nil
	}
	return a, err
}

func GetActivitiesForNode(ctx context.Context, db *gorm.DB, nodeId uuid.UUID, fleetId uuid.UUID, tenantId uuid.UUID) (a []FleetActivity, err error) {
	err = db.WithContext(ctx).Find(&a, "fleet_id = ? AND node_id = ? AND tenant_id = ?", fleetId, nodeId, tenantId).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return []FleetActivity{}, nil
	}
	return a, err
}

func (a *FleetActivity) CreateOrUpdateActivity(ctx context.Context, db *gorm.DB) error {
	var oldAct FleetActivity
	err := db.WithContext(ctx).Find(&oldAct, "fleet_id = ? AND node_id = ? AND tenant_id = ? AND activity_type = ? AND entity_id = ?", a.FleetId, a.NodeId, a.TenantId, a.ActivityType, a.EntityId).Error
	if errors.Is(err, gorm.ErrRecordNotFound) || oldAct.ID == uuid.Nil {
		return a.RecordActivity(ctx, db, a.TenantId)
	} else {
		return a.UpdateActivity(ctx, db, a.TenantId)
	}
}
