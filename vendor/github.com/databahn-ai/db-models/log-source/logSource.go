package log_source

import (
	"context"
	"time"

	checkpoint "github.com/databahn-ai/db-models/log-source-checkpoint"
	config "github.com/databahn-ai/db-models/log-source-config"
	"github.com/databahn-ai/db-models/utils"
	uuid "github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type LogSource struct {
	ID                        uuid.UUID `gorm:"primaryKey;type:uuid"`
	CreatedAt                 time.Time
	HistoryVersion            int
	UpdatedAt                 time.Time
	Name                      string         `gorm:"uniqueIndex:unique_name_per_tenant;type:VARCHAR(100)" validate:"required"`
	Description               string         `gorm:"type:VARCHAR(512)"`
	Schedule                  string         `gorm:"type:VARCHAR(36);default:'realtime'"`
	Device                    string         `gorm:"type:VARCHAR(30)"`
	Type                      string         `gorm:"type:VARCHAR(30)" validate:"required"`
	Version                   string         `gorm:"type:VARCHAR(36)" validate:"required"`
	Vendor                    string         `gorm:"type:VARCHAR(30)" validate:"required"`
	Filter                    string         `gorm:"type:VARCHAR(70)"`
	Port                      int            `validate:"min=0,max=65500"`
	Protocol                  string         `validate:"required" gorm:"type:VARCHAR(10)"`
	ConnectorID               uuid.UUID      `gorm:"type:uuid"`
	EdgeId                    uuid.UUID      `gorm:"type:uuid"`
	FleetId                   uuid.UUID      `gorm:"type:uuid"`
	ConfiguredLogSourceTypeId uuid.UUID      `gorm:"type:uuid"`
	TenantUUID                uuid.UUID      `gorm:"uniqueIndex:unique_name_per_tenant;type:uuid" json:"-"`
	DeletedAt                 gorm.DeletedAt `gorm:"index"`
	StatsLastUpdated          time.Time      `gorm:"autoCreateTime"`
	Status                    int
	EventCollected            int64
	EventDelivered            datatypes.JSON
	Timezone                  string
	Scope                     string `gorm:"type:VARCHAR(16);default:EDGE" validate:"required"`
	Configuration             datatypes.JSON
	CreatedBy                 uuid.UUID `gorm:"type:uuid"`
	UpdatedBy                 uuid.UUID `gorm:"type:uuid"`
	TimezoneNormalization     string    `gorm:"type:VARCHAR(16);default:DISABLED" validate:"oneof=DISABLED LOOKUP DEFAULT"`
	TimestampOverride         bool
	CheckPoint                checkpoint.LogSourceCheckPoint `gorm:"foreignKey:LogSourceID;references:ID"`
	LogSourceConfigs          []config.LogSourceConfig       `gorm:"foreignKey:LogSourceID;references:ID"`
	FetchMechanism            string                         `gorm:"type:VARCHAR(32)"`
	Reputation                int                            `validate:"min=0,max=2""`
	ReplaySource              bool
}

func (ls *LogSource) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(ls)
}

func Get(ctx context.Context, db *gorm.DB, lId string, tenantId uuid.UUID) (ls LogSource, err error) {
	err = db.WithContext(ctx).Where("id=? AND tenant_uuid = ?", lId, tenantId).Find(&ls).Error
	if err == gorm.ErrRecordNotFound {
		ls = LogSource{}
		return ls, nil
	}
	return ls, err
}

func GetAll(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (ls []LogSource, err error) {
	err = db.WithContext(ctx).Where("tenant_uuid = ?", tenantId).Find(&ls).Error
	if err == gorm.ErrRecordNotFound {
		ls = []LogSource{}
		return ls, nil
	}
	return ls, err
}

func SetStatus(ctx context.Context, db *gorm.DB, id uuid.UUID, tenantId uuid.UUID, status int) error {
	return db.WithContext(ctx).Where("tenant_uuid = ? AND id = ? ", tenantId, id).UpdateColumn("status", status).Error
}

func GetByStatus(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, status int) (ls []LogSource, err error) {
	err = db.WithContext(ctx).Where("tenant_uuid = ? and status = ?", tenantId, status).Find(&ls).Error
	if err == gorm.ErrRecordNotFound {
		ls = []LogSource{}
		return ls, nil
	}
	return ls, err
}

func GetAllForEdge(ctx context.Context, db *gorm.DB, aId string, tenantId uuid.UUID) (ls []LogSource, err error) {
	err = db.WithContext(ctx).Where("edge_id = ? AND tenant_uuid = ?", aId, tenantId).Find(&ls).Error
	if err == gorm.ErrRecordNotFound {
		ls = []LogSource{}
		return ls, nil
	}
	return ls, err
}

func GetAllForConnector(ctx context.Context, db *gorm.DB, cId string, tenantId uuid.UUID) (ls []LogSource, err error) {
	err = db.WithContext(ctx).Where("connector_id = ? AND tenant_uuid = ?", cId, tenantId).Find(&ls).Error
	if err == gorm.ErrRecordNotFound {
		ls = []LogSource{}
		return ls, nil
	}
	return ls, err
}

func (ls *LogSource) Update(ctx context.Context, db *gorm.DB, updated LogSource) error {
	return db.WithContext(ctx).Model(&ls).Updates(updated).Error
}

func DeleteLogSource(ctx context.Context, db *gorm.DB, lsId uuid.UUID, tenantId uuid.UUID) error {
	return db.Where("id = ? AND tenant_uuid = ?", lsId, tenantId).Delete(&LogSource{
		ID:         lsId,
		TenantUUID: tenantId,
	}).Error
}

func (ls *LogSource) Save(ctx context.Context, db *gorm.DB) (err error) {
	if ls.ID == uuid.Nil {
		ls.ID = uuid.New()
	}
	err = utils.IsValid(ls)
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Create(&ls).Error
}

func IsPortUsed(ctx context.Context, db *gorm.DB, tenantId uuid.UUID, edgeId uuid.UUID, connectorId uuid.UUID, port int) (ls int64, err error) {
	err = db.WithContext(ctx).Model(&LogSource{}).Where("tenant_uuid = ? AND edge_id = ? AND connector_id = ? AND port = ?", tenantId, edgeId, connectorId, port).Count(&ls).Error
	return ls, err
}
