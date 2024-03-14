package connector

import (
	"context"
	"errors"
	"time"

	log_source "github.com/databahn-ai/db-models/log-source"
	"github.com/databahn-ai/db-models/utils"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Connector struct {
	ID                 uuid.UUID `gorm:"primarykey;type:uuid"`
	EdgeId             uuid.UUID `gorm:"type:uuid"`
	FleetId            uuid.UUID `gorm:"type:uuid"`
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CreatedBy          uuid.UUID `gorm:"type:uuid"`
	Name               string    `gorm:"type:VARCHAR(100);uniqueIndex:unique_name_per_tenant" validate:"required"`
	Description        string    `gorm:"type:VARCHAR(512)"`
	Type               string    `validate:"required"`
	Status             int       `gorm:"index"`
	TenantUUID         uuid.UUID `gorm:"type:uuid;uniqueIndex:unique_name_per_tenant" json:"-"`
	HeartbeatAt        time.Time
	DeletedAt          gorm.DeletedAt `gorm:"index"`
	Configuration      datatypes.JSON
	Usage              datatypes.JSON
	Version            string
	IsUpgradeAvailable bool
	LastUpgraded       time.Time
}

type Syslog struct {
	ID         uuid.UUID
	LogSources []log_source.LogSource
	Type       string
	GatewayUrl string
	TenantUUID uuid.UUID
	RepoAlias  string
	Version    string
}

func (a *Connector) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(Connector{})
}

// Get connector by given connectorId and tenantId
func Get(ctx context.Context, db *gorm.DB, cId string, tenantId uuid.UUID) (connector Connector, err error) {
	err = db.WithContext(ctx).Where("id=? AND tenant_uuid = ?", cId, tenantId).Find(&connector).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		connector = Connector{}
		return connector, nil
	}
	return connector, err
}

// GetAll returns connectors for given tenant
func GetAll(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (connectors []Connector, err error) {
	err = db.WithContext(ctx).Where("tenant_uuid = ?", tenantId).Find(&connectors).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		connectors = []Connector{}
		return connectors, nil
	}
	return connectors, err
}

// GetByStatus returns connectors by status
func GetByStatus(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, status int) (connectors []Connector, err error) {
	err = db.WithContext(ctx).Where("tenant_uuid = ? and status = ?", tenantId, status).Find(&connectors).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		connectors = []Connector{}
		return connectors, nil
	}
	return connectors, err
}

// GetAllForEdge returns all connector for given edge
func GetAllForEdge(ctx context.Context, db *gorm.DB, aId string, tenantId uuid.UUID) (connectors []Connector, err error) {
	err = db.WithContext(ctx).Where("edge_id = ? AND tenant_uuid = ?", aId, tenantId).Find(&connectors).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		connectors = []Connector{}
		return connectors, nil
	}
	return connectors, err
}

// GetAllForTenantByUpgradeAvailable returns all connectors for which upgrade is available
func GetAllForTenantByUpgradeAvailable(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (fleetConnectors []Connector, err error) {
	err = db.WithContext(ctx).Where("tenant_uuid = ? and upgrade_available = ?", tenantId, true).Find(&fleetConnectors).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		fleetConnectors = []Connector{}
		return fleetConnectors, nil
	}
	return fleetConnectors, err
}

// UpdateVersionForTenant updates the verison for given connector
func UpdateVersionForTenant(ctx context.Context, db *gorm.DB, cId string, tenantId uuid.UUID, version string) error {
	return db.WithContext(ctx).Model(&Connector{}).Where("id = ? and tenant_uuid = ?", cId, tenantId).UpdateColumns(Connector{Version: version}).Error
}

// GetAllForFleet returns all connector for given fleet
func GetAllForFleet(ctx context.Context, db *gorm.DB, aId string, tenantId uuid.UUID) (fleetConnectors []Connector, err error) {
	err = db.WithContext(ctx).Where("fleet_id = ? AND tenant_uuid = ?", aId, tenantId).Find(&fleetConnectors).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		fleetConnectors = []Connector{}
		return fleetConnectors, nil
	}
	return fleetConnectors, err
}
func (a *Connector) Save(ctx context.Context, db *gorm.DB) (err error) {
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	err = utils.IsValid(a)
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Create(&a).Error
}

func (a *Connector) Update(ctx context.Context, db *gorm.DB, updated Connector) (err error) {
	err = db.WithContext(ctx).Model(&a).Updates(updated).Error
	return err
}

func (a *Connector) UpdateColum(ctx context.Context, db *gorm.DB, column string, value interface{}) error {
	return db.WithContext(ctx).Model(&a).Where("id = ? AND tenant_uuid = ?", a.ID, a.TenantUUID).Update(column, value).Error
}

func (a *Connector) Delete(ctx context.Context, db *gorm.DB) (err error) {
	err = db.WithContext(ctx).Delete(&a).Error
	return err
}

func (a *Connector) UpdateUsage(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Model(&a).Where("id = ?", a.ID).UpdateColumns(Connector{Usage: a.Usage}).Error
}
