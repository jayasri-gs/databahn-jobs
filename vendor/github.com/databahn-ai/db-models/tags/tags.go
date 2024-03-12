package tags

import (
	"context"
	"time"

	"github.com/databahn-ai/db-models/utils"
	uuid "github.com/google/uuid"
	"gorm.io/gorm"
)

type Tags struct {
	ID         int       `gorm:"primaryKey;autoIncrement:true"`
	EntityId   uuid.UUID `gorm:"type:uuid" validate:"required"`
	TenantUUID uuid.UUID `gorm:"type:uuid" json:"-"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
	EntityType string `gorm:"type:VARCHAR(15)" validate:"required"`
	Tag        string `gorm:"type:VARCHAR(50)" validate:"required"`
}

func (dt *Tags) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(dt)
}

func GetByColumn(ctx context.Context, db *gorm.DB, column string, value any) (d []Tags, err error) {
	err = db.WithContext(ctx).Where(column, value).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []Tags{}
		return d, nil
	}
	return d, err
}

func GetTag(ctx context.Context, db *gorm.DB, id int, tenantId string) (dt Tags, err error) {
	err = db.WithContext(ctx).Where("id=? AND tenant_uuid=?", id, tenantId).Find(&dt).Error
	return dt, err
}

func GetTagsByEntity(ctx context.Context, db *gorm.DB, entityId string, entityType string) (dt []Tags, err error) {
	err = db.WithContext(ctx).Where("entity_id=? AND entity_type=?", entityId, entityType).Find(&dt).Error
	if err == gorm.ErrRecordNotFound {
		dt = []Tags{}
		return dt, nil
	}
	return dt, err
}

func GetAllTagsByTenant(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (dt []Tags, err error) {
	err = db.WithContext(ctx).Where("tenant_uuid=?", tenantId).Find(&dt).Error
	if err == gorm.ErrRecordNotFound {
		dt = []Tags{}
		return dt, nil
	}
	return dt, err
}

func (dt *Tags) Update(ctx context.Context, db *gorm.DB, updated Tags) error {
	return db.WithContext(ctx).Model(&dt).Updates(updated).Error
}

func DeleteTag(ctx context.Context, db *gorm.DB, dtid int) error {
	return db.WithContext(ctx).Delete(&Tags{
		ID: dtid,
	}).Error
}

func (dt *Tags) Save(ctx context.Context, db *gorm.DB) (err error) {
	err = utils.IsValid(dt)
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Create(&dt).Error
}
