package destination_config

import (
	"context"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type DestinationConfig struct {
	ID            uuid.UUID `gorm:"primarykey;type:uuid"`
	DestinationID uuid.UUID `gorm:"type:uuid; index"`
	CreatedBy     string    `gorm:"type:VARCHAR(100)" validate:"required"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
	FieldKey      string `gorm:"type:VARCHAR(100)" validate:"required"`
	FieldValue    string `gorm:"type:TEXT" validate:"required"`
	IsSecret      bool   `gorm:"default:false" validate:"required"`
}

func (d *DestinationConfig) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(d)
}

func GetAll(ctx context.Context, db *gorm.DB) (d []DestinationConfig, err error) {
	err = db.WithContext(ctx).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []DestinationConfig{}
		return d, nil
	}
	return d, err
}

func Get(ctx context.Context, db *gorm.DB, dcId uuid.UUID) (d []DestinationConfig, err error) {
	err = db.WithContext(ctx).Where(&d, "id = ? ", dcId).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []DestinationConfig{}
		return d, nil
	}
	return d, err
}

func (d *DestinationConfig) Update(ctx context.Context, db *gorm.DB, updated DestinationConfig) error {
	return db.WithContext(ctx).Model(&d).Updates(updated).Error
}

func (d *DestinationConfig) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(&d).Error
}

func (d *DestinationConfig) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Delete(&d).Error
}

func DeleteConfigsForDestination(ctx context.Context, db *gorm.DB, dId uuid.UUID) error {
	return db.WithContext(ctx).Unscoped().Where("destination_id = ?", dId).Delete(DestinationConfig{}).Error
}
