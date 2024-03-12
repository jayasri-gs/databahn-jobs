package data_transform_function

import (
	"context"
	"gorm.io/datatypes"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type DataTransformFunction struct {
	ID              uuid.UUID `gorm:"primarykey;type:uuid"`
	DataTransformId uuid.UUID `gorm:"type:uuid; index"`
	Type            string
	Fields          datatypes.JSON
}

func (d *DataTransformFunction) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(d)
}

func Get(ctx context.Context, db *gorm.DB, dfId uuid.UUID) (d []DataTransformFunction, err error) {
	err = db.WithContext(ctx).Where(&d, "id = ? ", dfId).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []DataTransformFunction{}
		return d, nil
	}
	return d, err
}

func (d *DataTransformFunction) Update(ctx context.Context, db *gorm.DB, updated DataTransformFunction) error {
	return db.WithContext(ctx).Model(&d).Updates(updated).Error
}

func (d *DataTransformFunction) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(&d).Error
}

func (d *DataTransformFunction) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Delete(&d).Error
}
