package log_source_checkpoint

import (
	"context"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type LogSourceCheckPoint struct {
	LogSourceID uuid.UUID `gorm:"primarykey;type:uuid;"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CheckPoint  string
}

func (cp *LogSourceCheckPoint) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(cp)
}

func Get(db *gorm.DB, lsId uuid.UUID) (cp *LogSourceCheckPoint, err error) {
	err = db.First(&cp, "log_source_id = ? ", lsId).Error
	return
}

func (cp *LogSourceCheckPoint) Update(ctx context.Context, db *gorm.DB, updated LogSourceCheckPoint) error {
	return db.WithContext(ctx).Model(&cp).Updates(updated).Error
}

func (cp *LogSourceCheckPoint) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(&cp).Error
}

func (cp *LogSourceCheckPoint) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Delete(&cp).Error
}
