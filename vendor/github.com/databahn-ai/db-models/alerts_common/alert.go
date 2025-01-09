package alerts_common

import (
	"context"
	"time"

	"github.com/databahn-ai/db-models/utils"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Alert struct {
	ID                      uuid.UUID `gorm:"primaryKey;type:uuid" json:"id"`
	Criticality             string    `gorm:"not null" validate:"required" json:"criticality"`
	Title                   string    `gorm:"not null;type:VARCHAR(128)" validate:"required" json:"title"`
	Message                 string    `gorm:"type:VARCHAR(512)" json:"message"`
	CreatedAt               time.Time `json:"createdAt,omitempty"`
	UpdatedAt               time.Time `json:"updatedAt,omitempty"`
	FirstObservedAt         time.Time `json:"firstObservedAt,omitempty"`
	LastObservedAt          time.Time `json:"lastObservedAt,omitempty"`
	TenantUUID              uuid.UUID `gorm:"type:uuid" validate:"required" json:"tenantId"`
	Functionality           string    `gorm:"not null;type:VARCHAR(64)" validate:"required" json:"functionality"`
	FunctionalityEntityId   string    `gorm:"type:VARCHAR(36)" json:"functionalityEntityId"`
	FunctionalityEntityName string    `gorm:"type:VARCHAR(128)" json:"functionalityEntityName"`
	FunctionalityType       string    `gorm:"type:VARCHAR(64)" json:"functionalityType"`
	Status                  int       `json:"status"`
	UpdatedBy               string    `gorm:"type:VARCHAR(100)" validate:"required"`
	Dismissed               bool      `json:"dismissed"`
	AlertType               string    `gorm:"type:VARCHAR(64)" json:"alertType"`
	ErrorMessage            string    `gorm:"type:text" json:"errorMessage"`
	ErrorCode               string    `gorm:"type:VARCHAR(128)" json:"errorCode"`
	DataPlaneId             uuid.UUID `gorm:"type:uuid" validate:"required" json:"dataPlaneId"`
}

func (at *Alert) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(at)
}

func GetAll(ctx context.Context, db *gorm.DB) (at []Alert, err error) {
	err = db.WithContext(ctx).Find(&at).Error
	if err == gorm.ErrRecordNotFound {
		at = []Alert{}
		return at, nil
	}
	return at, err
}
func GetAllForTenant(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (at []Alert, err error) {
	err = db.WithContext(ctx).Find(&at, "tenant_uuid = ?", tenantId).Error
	if err == gorm.ErrRecordNotFound {
		at = []Alert{}
		return at, nil
	}
	return at, err
}
func Get(ctx context.Context, db *gorm.DB, atId uuid.UUID) (at *Alert, err error) {
	err = db.WithContext(ctx).First(&at, "id = ? ", atId).Error
	if err == gorm.ErrRecordNotFound {
		at = &Alert{}
		return at, nil
	}
	return at, err
}
func GetByColumnForTenant(ctx context.Context, db *gorm.DB, column string, value any, tenantId uuid.UUID) (at []Alert, err error) {
	err = db.WithContext(ctx).Where(column, value).Where("tenant_uuid = ?", tenantId).Find(&at).Error
	if err == gorm.ErrRecordNotFound {
		at = []Alert{}
		return at, nil
	}
	return at, err
}

func (at *Alert) Update(ctx context.Context, db *gorm.DB, updated Alert) error {
	return db.WithContext(ctx).Model(&at).Updates(updated).Error
}

func (at *Alert) Save(ctx context.Context, db *gorm.DB) (err error) {
	if at.ID == uuid.Nil {
		at.ID = uuid.New()
	}
	err = utils.IsValid(at)
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Create(&at).Error
}

func (at *Alert) DismissAlert(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Model(&at).Updates(Alert{UpdatedAt: time.Now(), UpdatedBy: "admin", Status: AlertDismissed}).Error
}

func (at *Alert) UpdateLastSeen(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Model(&at).Updates(Alert{LastObservedAt: time.Now()}).Error
}
