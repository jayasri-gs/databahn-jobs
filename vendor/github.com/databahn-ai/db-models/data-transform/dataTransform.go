package data_transform

import (
	"context"
	"time"

	data_transform_function "github.com/databahn-ai/db-models/data-transform-function"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type DataTransform struct {
	ID                     uuid.UUID `gorm:"primarykey;type:uuid"`
	Name                   string    `gorm:"type:VARCHAR(100);uniqueIndex:unique_name_per_tenant" validate:"required"`
	HistoryVersion         int
	Description            string    `gorm:"type:VARCHAR(512)"`
	SourceId               uuid.UUID `gorm:"type:uuid; index"`
	DestinationId          uuid.UUID `gorm:"type:uuid; index"`
	EventDependencyId      uuid.UUID `gorm:"type:uuid; uniqueIndex"`
	TenantUUID             uuid.UUID `gorm:"type:uuid;uniqueIndex:unique_name_per_tenant" json:"-"`
	Status                 int
	CreatedBy              string `gorm:"type:VARCHAR(100)" validate:"required"`
	UpdatedBy              string `gorm:"type:VARCHAR(100)" validate:"required"`
	CreatedAt              time.Time
	UpdatedAt              time.Time
	DeletedAt              gorm.DeletedAt
	TransformationType     string                                        `gorm:"type:VARCHAR(512)"`
	TransformationOutput   string                                        `gorm:"type:VARCHAR(512)"`
	DataTransformFunctions data_transform_function.DataTransformFunction `gorm:"foreignKey:DataTransformId;references:ID"`
	ProcessedRawEvent      bool
}

func (d *DataTransform) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(d)
}

func Get(ctx context.Context, db *gorm.DB, dId uuid.UUID) (d *DataTransform, err error) {
	err = db.WithContext(ctx).First(&d, "id = ? ", dId).Error
	if err == gorm.ErrRecordNotFound {
		d = &DataTransform{}
		return d, nil
	}
	return d, err
}
func GetBySourceId(ctx context.Context, db *gorm.DB, sId uuid.UUID) (d []DataTransform, err error) {
	err = db.WithContext(ctx).Find(&d, "source_id = ?", sId).Error
	if err == gorm.ErrRecordNotFound {
		d = []DataTransform{}
		return d, nil
	}
	return d, err
}
func GetByDestinationId(ctx context.Context, db *gorm.DB, dId uuid.UUID) (d []DataTransform, err error) {
	err = db.WithContext(ctx).Find(&d, "destination_id = ?", dId).Error
	if err == gorm.ErrRecordNotFound {
		d = []DataTransform{}
		return d, nil
	}
	return d, err
}

func GetAll(ctx context.Context, db *gorm.DB) (d []DataTransform, err error) {
	err = db.WithContext(ctx).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []DataTransform{}
		return d, nil
	}
	return d, err
}

func (d *DataTransform) Update(ctx context.Context, db *gorm.DB, updated DataTransform) error {
	return db.WithContext(ctx).Model(&d).Updates(updated).Error
}

func (d *DataTransform) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(&d).Error
}

func (d *DataTransform) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Delete(&d).Error
}
