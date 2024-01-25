package log_source_config

import (
	"context"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type LogSourceConfig struct {
	ID          uuid.UUID `gorm:"primarykey;type:uuid"`
	LogSourceID uuid.UUID `gorm:"type:uuid; index"`
	CreatedBy   string    `gorm:"type:VARCHAR(100)" validate:"required"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	FieldKey    string `gorm:"type:VARCHAR(100)" validate:"required"`
	FieldValue  string `gorm:"type:TEXT" validate:"required"`
	IsSecret    bool   `gorm:"default:false" validate:"required"`
}

func (ls *LogSourceConfig) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(ls)
}

func GetAll(ctx context.Context, db *gorm.DB) (ls []LogSourceConfig, err error) {
	err = db.WithContext(ctx).Find(&ls).Error
	if err == gorm.ErrRecordNotFound {
		ls = []LogSourceConfig{}
		return ls, nil
	}
	return ls, err
}

func Get(ctx context.Context, db *gorm.DB, lsId uuid.UUID) (ls []LogSourceConfig, err error) {
	err = db.WithContext(ctx).Where(&ls, "id = ? ", lsId).Find(&ls).Error
	if err == gorm.ErrRecordNotFound {
		ls = []LogSourceConfig{}
		return ls, nil
	}
	return ls, err
}

func (ls *LogSourceConfig) Update(ctx context.Context, db *gorm.DB, updated LogSourceConfig) error {
	return db.WithContext(ctx).Model(&ls).Updates(updated).Error
}

func (ls *LogSourceConfig) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(&ls).Error
}

func (ls *LogSourceConfig) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Delete(&ls).Error
}
func DeleteConfigsForLogSource(ctx context.Context, db *gorm.DB, lsId uuid.UUID) error {
	return db.WithContext(ctx).Unscoped().Where("log_source_id = ?", lsId).Delete(LogSourceConfig{}).Error
}
