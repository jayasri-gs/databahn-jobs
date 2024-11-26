package helper

import (
	"context"
	"errors"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

// EntityAlertsConfig represents the entity_alerts_config table
type EntityAlertsConfig struct {
	Summary         string    `gorm:"-"`
	TenantName      string    `gorm:"-"`
	EntityName      string    `gorm:"type:varchar(255)"`
	EntityType      string    `gorm:"type:varchar(255)"`
	LastCheckedTime time.Time `gorm:"type:timestamp"`
	ID              uuid.UUID `gorm:"type:uuid;primary_key"`
	Interval        int       `gorm:"type:int"`
	EntityID        uuid.UUID `gorm:"type:uuid"`
	TenantID        uuid.UUID `gorm:"type:uuid"`
	CustomerD       uuid.UUID `gorm:"type:uuid"`
	Criticality     string    `gorm:"type:varchar(255)"`
	Status          bool      `gorm:"type:boolean"`
	EnvType         string    `gorm:"type:varchar(255);default:'DEV'"`
	CpGatewayUrl    string    `gorm:"type:varchar(255);default:'gateway.galaxy.dabahn.app'"`
	DpGatewayUrl    string    `gorm:"type:varchar(255);default:'gateway-galaxy-dp01-nonprod.databahn.app'"`
}

// QueryEntityAlertsConfig queries the entity_alerts_config table based on tenantId, interval, and lastcheckedtime
func QueryEntityAlertsConfig(db *gorm.DB, tenantId uuid.UUID, interval int, lastCheckedTime time.Time) ([]EntityAlertsConfig, error) {
	var results []EntityAlertsConfig
	err := config.GetDB().Where("tenant_id = ? AND interval = ? AND lastcheckedtime = ?", tenantId, interval, lastCheckedTime).Find(&results).Error
	return results, err
}

func CreateEntityAlertsConfigMapByType(db *gorm.DB) (map[string]EntityAlertsConfig, error) {
	var configs []EntityAlertsConfig
	err := db.Find(&configs).Error
	if err != nil {
		return nil, err
	}

	configMap := make(map[string]EntityAlertsConfig)
	for _, config := range configs {
		key := fmt.Sprintf("%s_%s", config.EntityType, config.EntityID.String())
		configMap[key] = config
	}

	return configMap, nil
}

func CreateEntityAlertsConfigMapByTenant(db *gorm.DB) (map[string]EntityAlertsConfig, error) {
	var configs []EntityAlertsConfig
	err := db.Find(&configs).Error
	if err != nil {
		return nil, err
	}

	configMap := make(map[string]EntityAlertsConfig)
	for _, config := range configs {
		key := fmt.Sprintf("%s_%s_%d", config.TenantID.String(), config.EntityType, config.Interval)
		configMap[key] = config
	}

	return configMap, nil
}

type Tenants struct {
	ID     uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4()"`
	Name   string    `gorm:"size:64;not null;unique"`
	Active bool      `gorm:"default:true"`
}

func GetTenant(ctx context.Context, db *gorm.DB, tenantId uuid.UUID) (tenant Tenants, err error) {
	err = db.WithContext(ctx).Where("id = ?", tenantId).Find(&tenant).Error
	if err == gorm.ErrRecordNotFound {
		tenant = Tenants{}
		return tenant, nil
	}
	return tenant, err
}

func GetAllTenants(ctx context.Context, db *gorm.DB) (tenants []Tenants, err error) {
	err = db.WithContext(ctx).Find(&tenants).Error
	if err == gorm.ErrRecordNotFound {
		tenants = []Tenants{}
		return tenants, nil
	}
	return tenants, err
}

type LogSource struct {
	ID          uuid.UUID  `gorm:"type:uuid"`
	Name        string     `gorm:"type:varchar(255)"`
	TenantID    uuid.UUID  `gorm:"type:uuid"`
	Status      string     `gorm:"type:varchar(255)"`
	DataPlaneID uuid.UUID  `gorm:"type:uuid"`
	DataPlanes  DataPlanes `gorm:"foreignKey:DataPlaneID;references:ID"`
}
type DataPlanes struct {
	ID           uuid.UUID `gorm:"type:uuid;primary_key"`
	Name         string    `gorm:"type:varchar(255)"`
	CPGatewayUrl string    `gorm:"type:varchar(255);not null"`
	DPGatewayUrl string    `gorm:"type:varchar(255);not null"`
}

// GetAllLogSources fetches all logSource records from the database
func GetAllLogSources(ctx context.Context, db *gorm.DB) ([]LogSource, error) {
	var logSources []LogSource
	err := db.WithContext(ctx).Preload("DataPlanes").Find(&logSources).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		logSources = []LogSource{}
		return logSources, nil
	}
	return logSources, err
}

// CreateLogSourceMapByTenant creates a map with keys as tenantId_logSourceId and values as LogSource objects
func CreateLogSourceMapByTenant(ctx context.Context, db *gorm.DB) (map[string]LogSource, error) {
	logSources, err := GetAllLogSources(ctx, db)
	if err != nil {
		return nil, err
	}

	logSourceMap := make(map[string]LogSource)
	for _, logsource := range logSources {
		key := fmt.Sprintf("%s_%s", logsource.TenantID.String(), logsource.ID.String())
		logSourceMap[key] = logsource
	}

	return logSourceMap, nil
}

func GetLogSourcesByIds(ctx context.Context, db *gorm.DB, ids []uuid.UUID) (map[string]LogSource, error) {
	var logSources []LogSource
	err := db.WithContext(ctx).Where("id IN ?", ids).Preload("DataPlanes").Find(&logSources).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		logSources = []LogSource{}
		return nil, err
	}
	logSourceMap := make(map[string]LogSource)
	for _, logsource := range logSources {
		key := fmt.Sprintf("%s_%s", logsource.TenantID.String(), logsource.ID.String())
		logSourceMap[key] = logsource
	}
	return logSourceMap, nil
}
