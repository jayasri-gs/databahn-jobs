package fleet

import (
	"context"
	"time"

	"github.com/google/uuid"

	"gorm.io/gorm"
)

type Fleet struct {
	ID             uuid.UUID `gorm:"primarykey;type:uuid"`
	Name           string    `gorm:"size:100;uniqueIndex:unique_name_per_tenant" validate:"required"`
	TenantUUID     uuid.UUID `gorm:"type:uuid;uniqueIndex:unique_name_per_tenant" validate:"required" json:"-"`
	CustomerId     string    `gorm:"type:VARCHAR(36)" validate:"required" json:"-"`
	Topology       string    `gorm:"size:10;default:normal" json:"topology"`
	Description    string    `gorm:"type:VARCHAR(512)"`
	Status         int       `validate:"lte=5"`
	CreatedBy      uuid.UUID `validate:"required" gorm:"type:uuid"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ClusterSecret  string                 `json:"cluster_secret"`
	AdditionalInfo map[string]interface{} `gorm:"type:jsonb"`
}

func (*Fleet) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(&Fleet{})
}

// Create function will save fleet entity in database
func (f *Fleet) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(f).Error
}

// Create function will update existing fleet entity in database
func (f *Fleet) Update(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Save(f).Error
}

func (f *Fleet) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Where("tenant_uuid = ? AND id = ?", f.TenantUUID, f.ID).Delete(f).Error
}

func GetFleets(ctx context.Context, db *gorm.DB, tenantId string) (fleets []*Fleet, mysqlErr error) {
	mysqlErr = db.WithContext(ctx).Find(&fleets, "tenant_uuid = ?", tenantId).Error
	return fleets, mysqlErr
}

func GetFleet(ctx context.Context, db *gorm.DB, fleetId uuid.UUID, tenantId uuid.UUID) (fleet *Fleet, err error) {
	err = db.WithContext(ctx).Where("id = ? AND tenant_uuid = ?", fleetId, tenantId).First(&fleet).Error
	if err == gorm.ErrRecordNotFound {
		return &Fleet{}, nil
	}
	return fleet, err
}

func StoreSecret(ctx context.Context, db *gorm.DB, fleetId, tenantId, secret string) error {
	return db.WithContext(ctx).Model(&Fleet{}).Where("id = ? AND tenant_uuid = ?", fleetId, tenantId).UpdateColumn("cluster_secret", secret).Error
}
