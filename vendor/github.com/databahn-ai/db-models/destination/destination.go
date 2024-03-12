package destination

import (
	"context"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type Destination struct {
	ID              uuid.UUID `gorm:"primarykey;type:uuid"`
	Name            string    `gorm:"type:VARCHAR(100);uniqueIndex:unique_name_per_tenant" validate:"required"`
	Description     string    `gorm:"type:VARCHAR(512)"`
	HistoryVersion  int
	TenantUUID      uuid.UUID `gorm:"type:uuid;uniqueIndex:unique_name_per_tenant" json:"-"`
	DestinationType string    `gorm:"type:VARCHAR(36)" validate:"required"`
	CreatedBy       string    `gorm:"type:VARCHAR(100)" validate:"required"`
	UpdatedBy       string    `gorm:"type:VARCHAR(100)" validate:"required"`
	Status          int       `validate:"min=0,max=5"`
	Enabled         bool
	ForwardDataType int `validate:"lte=3"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
	HeartBeatAt     time.Time
	DeletedAt       gorm.DeletedAt `gorm:"index"`
	Scope           string         `gorm:"type:VARCHAR(16);default:CLOUD" validate:"required"`
	EntityId        string         `gorm:"type:VARCHAR(36)"`
}

func (d *Destination) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(d)
}

func Get(ctx context.Context, db *gorm.DB, dId uuid.UUID, tenantId uuid.UUID) (d *Destination, err error) {
	err = db.WithContext(ctx).First(&d, "id = ? and tenant_uuid = ?", dId, tenantId).Error
	if err == gorm.ErrRecordNotFound {
		d = &Destination{}
		return d, nil
	}
	return d, err
}

func GetAllByTenant(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (d []Destination, err error) {
	err = db.WithContext(ctx).Find(&d, "tenant_uuid = ?", tenantId).Error
	if err == gorm.ErrRecordNotFound {
		d = []Destination{}
		return d, nil
	}
	return d, err
}

func GetDestinationByEntityId(ctx context.Context, db *gorm.DB, entityId string, tenantId string) (d []Destination, err error) {
	err = db.WithContext(ctx).Find(&d, "entity_id = ? AND tenant_id = ?", entityId, tenantId).Error
	if err == gorm.ErrRecordNotFound {
		d = []Destination{}
		return d, nil
	}
	return d, err
}

func GetByStatusByScopeByTenant(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, scope string, status int) (d []Destination, err error) {
	err = db.WithContext(ctx).Where("tenant_uuid = ? AND scope = ? AND enabled = ?", tenantId, scope, status).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []Destination{}
		return d, nil
	}
	return d, err
}
func GetAll(ctx context.Context, db *gorm.DB) (d []Destination, err error) {
	err = db.WithContext(ctx).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []Destination{}
		return d, nil
	}
	return d, err
}

func GetByType(ctx context.Context, db *gorm.DB, destinationType string) (d []Destination, err error) {
	err = db.WithContext(ctx).Find(&d, "destination_type = ?", destinationType).Error
	if err == gorm.ErrRecordNotFound {
		d = []Destination{}
		return d, nil
	}
	return d, err
}

func DisableDestination(ctx context.Context, db *gorm.DB, destinationId string, tenantId string) error {
	return db.WithContext(ctx).Model(&Destination{}).Where("tenant_uuid = ? AND id = ?", tenantId, destinationId).Update("Enabled", false).Error
}

func (d *Destination) Update(ctx context.Context, db *gorm.DB, updated Destination) error {
	return db.WithContext(ctx).Model(&d).Updates(updated).Error
}

func (d *Destination) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(&d).Error
}

func (d *Destination) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Delete(&d).Error
}
