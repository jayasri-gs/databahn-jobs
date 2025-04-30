package event_dependency

import (
	"context"
	"time"

	"github.com/databahn-ai/db-models/destination"
	"github.com/databahn-ai/db-models/utils"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type EventDependency struct {
	ID                        uuid.UUID `gorm:"UNIQUE;type:uuid"`
	DestinationId             uuid.UUID `gorm:"primarykey;type:uuid;index:idx_event,priority:1"`
	EventSourceId             uuid.UUID `gorm:"primarykey;type:uuid;index:idx_event,priority:2"`
	TenantUUID                uuid.UUID `gorm:"primarykey;type:uuid;index:idx_event,priority:3"`
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
	DeletedAt                 gorm.DeletedAt `gorm:"index"`
	DestinationOverrideConfig datatypes.JSON
	DestinationOverride       bool
	FormatOverride            int `validate:"lte=3"`
	FormatOverrideFlag        bool
}

func (d *EventDependency) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(d)
}

func Get(ctx context.Context, db *gorm.DB, id uuid.UUID, tenantId uuid.UUID) (ed *EventDependency, err error) {
	err = db.WithContext(ctx).First(&ed, "id = ? and tenant_uuid = ?", id, tenantId).Error
	return
}

func GetByColumn(ctx context.Context, db *gorm.DB, column string, value any, tenantId uuid.UUID) (d []EventDependency, err error) {
	err = db.WithContext(ctx).Where(column, value).Where("tenant_uuid = ?", tenantId).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []EventDependency{}
		return d, nil
	}
	return d, err
}

func GetAllByTenant(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (d []EventDependency, err error) {
	err = db.WithContext(ctx).Find(&d, "tenant_uuid = ?", tenantId).Error
	if err == gorm.ErrRecordNotFound {
		d = []EventDependency{}
		return d, nil
	}
	return d, err
}

func GetAll(ctx context.Context, db *gorm.DB) (d []EventDependency, err error) {
	err = db.WithContext(ctx).Find(&d).Error
	if err == gorm.ErrRecordNotFound {
		d = []EventDependency{}
		return d, nil
	}
	return d, err
}

func (d *EventDependency) Update(ctx context.Context, db *gorm.DB, updated EventDependency) error {
	return db.WithContext(ctx).Model(&d).Updates(updated).Error
}

func (d *EventDependency) Create(ctx context.Context, db *gorm.DB) (err error) {
	err = utils.IsValid(d)
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Create(&d).Error
}

func GetAllSourcesByDestinations(ctx context.Context, db *gorm.DB, destinationId string, tenantId string) (destinations []destination.Destination, err error) {
	err = db.WithContext(ctx).Table("db_log_source").
		Select("db_log_source.*").
		Joins("JOIN db_event_dependency ON db_log_source.id = db_event_dependency.event_source_id").
		Where("db_event_dependency.destination_id = ? AND db_event_dependency.tenant_uuid = ?", destinationId, tenantId).
		Find(&destinations).Error

	return destinations, err
}

type DestinationWithRuleCount struct {
	destination.Destination
	RuleCount int
}

func GetAllDestinationsBySource(ctx context.Context, db *gorm.DB, sourceId string, tenantId string) (destinations []DestinationWithRuleCount, err error) {
	err = db.WithContext(ctx).Table("db_destination").
		Select("db_destination.*, count(r.id) as \"RuleCount\"").
		Joins("JOIN db_event_dependency ON db_destination.id = db_event_dependency.destination_id").
		Joins("LEFT JOIN db_rule r ON r.event_dependency_id = db_event_dependency.id").
		Where("db_event_dependency.event_source_id = ? AND db_event_dependency.tenant_uuid = ?", sourceId, tenantId).
		Group("db_destination.id").
		Find(&destinations).Error

	return destinations, err
}

func GetRelationship(ctx context.Context, db *gorm.DB, sourceId uuid.UUID, destId uuid.UUID, tenantId uuid.UUID) (ed *EventDependency, err error) {
	err = db.WithContext(ctx).Where("event_source_id = ? AND destination_id=? AND  tenant_uuid = ?", sourceId, destId, tenantId).First(&ed).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ed, nil
}

func (d *EventDependency) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Delete(&d).Error
}
