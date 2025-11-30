package entities

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Interval string

const Minute Interval = "MINUTE"
const Hour Interval = "HOUR"
const Day Interval = "DAY"
const Week Interval = "WEEK"

type IncludeExclude string

const Include IncludeExclude = "INCLUDE"
const Exclude IncludeExclude = "EXCLUDE"

const LogSourceEntityType = "LOG_SOURCE"
const DestinationEntityType = "DESTINATION"
const TenantEntityType = "TENANT"

// sandboxAlertCache holds cached results of ShouldSkipSandboxAlerts per tenant
// Key: tenant UUID string, Value: sandboxAlertCacheEntry
var sandboxAlertCache sync.Map

// sandboxAlertCacheEntry represents a cached entry for sandbox alert configuration
type sandboxAlertCacheEntry struct {
	ShouldSkip bool
	Error      error
}

type Reputation string

const (
	ReputationSilent     Reputation = "SILENT"
	ReputationStable     Reputation = "STABLE"
	ReputationWhispering Reputation = "WHISPERING"
	ReputationNoisy      Reputation = "NOISY"
)

type AlertConfig struct {
	Enabled                             bool                                 `json:"enabled"`
	LogSourceInactivityAlertConfig      *LogSourceInactivityAlertConfig      `json:"logSourceInactivityAlertConfig"`
	DestinationInactivityAlertConfig    *DestinationInactivityAlertConfig    `json:"destinationInactivityAlertConfig"`
	LogSourceDeviceInventoryAlertConfig *LogSourceDeviceInventoryAlertConfig `json:"logSourceDeviceInventoryAlertConfig"`
	DestinationDeliveredMoreAlertConfig *DestinationDeliveredMoreAlertConfig `json:"destinationDeliveredMoreAlertConfig"`
	DisableSandboxAlertsConfig          *DisableSandboxAlertsConfig          `json:"disableSandboxAlertsConfig"`
}

func (a *AlertConfig) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to scan AlertConfig: not []byte")
	}
	return json.Unmarshal(bytes, a)
}

func (a *AlertConfig) Value() (driver.Value, error) {
	return json.Marshal(a)
}

type LogSourceInactivityAlertConfig struct {
	InactivityDuration *Duration `json:"inactivityDuration"`
}

type DestinationInactivityAlertConfig struct {
	InactivityDuration *Duration `json:"inactivityDuration"`
}

type LogSourceDeviceInventoryAlertConfig struct {
	ReputationsToAlert []Reputation   `json:"reputationsToAlert"`
	VcRuleFilters      *VcRuleFilter  `json:"vcRuleFilters"`
	IncludeExclude     IncludeExclude `json:"includeExclude"`
}

type DestinationDeliveredMoreAlertConfig struct {
	DifferencePercentageThreshold   int    `json:"differencePercentageThreshold"`
	MinimumIngestionVolumeThreshold int64  `json:"minimumIngestionVolumeThreshold"`
	MinimumIngestionVolumeUnit      string `json:"minimumIngestionVolumeUnit"`
}

type DisableSandboxAlertsConfig struct {
	// This config uses the AlertConfig.Enabled field to control whether sandbox alerts are disabled
	// No additional fields are needed
}

type VcRuleFilter struct {
	Field      string         `json:"field"`
	Value      string         `json:"value"`
	Operator   string         `json:"operator"`
	Rules      []VcRuleFilter `json:"rules"`
	Combinator string         `json:"combinator"`
}

type Duration struct {
	Time     int      `json:"time"`
	Interval Interval `json:"interval"`
}

func (d *Duration) GetDuration() (time.Duration, error) {
	switch d.Interval {
	case Minute:
		return time.Duration(d.Time) * time.Minute, nil
	case Hour:
		return time.Duration(d.Time) * time.Hour, nil
	case Day:
		return time.Duration(d.Time) * time.Hour * 24, nil
	case Week:
		return time.Duration(d.Time) * time.Hour * 24 * 7, nil
	default:
		return 0, fmt.Errorf("invalid interval: %s", d.Interval)
	}
}

type EntityAlertsConfig struct {
	ID         uuid.UUID    `gorm:"type:uuid;primary_key;column:id"`
	EntityID   uuid.UUID    `gorm:"type:uuid;column:entity_id"`
	EntityType string       `gorm:"type:varchar(255);column:entity_type"`
	AlertType  string       `gorm:"type:varchar(255);column:alert_type"`
	TenantID   uuid.UUID    `gorm:"type:uuid;column:tenant_id"`
	CustomerID uuid.UUID    `gorm:"type:uuid;column:customer_id"`
	Config     *AlertConfig `gorm:"type:json;column:config"`
	CreatedBy  uuid.UUID    `gorm:"type:uuid;column:created_by"`
	UpdatedBy  uuid.UUID    `gorm:"type:uuid;column:updated_by"`
	CreatedAt  time.Time    `gorm:"column:created_at"`
	UpdatedAt  time.Time    `gorm:"column:updated_at"`
}

// TableName specifies the table name for GORM
func (EntityAlertsConfig) TableName() string {
	return "entity_alerts_config"
}

func ReadEntityConfigs(db *gorm.DB, entityType, alertType string, tenantId uuid.UUID, entityIds []uuid.UUID) ([]EntityAlertsConfig, error) {
	var entityConfigs []EntityAlertsConfig
	err := db.Where("tenant_id = ? AND entity_type = ? AND alert_type = ? AND entity_id IN ?", tenantId, entityType, alertType, entityIds).
		Find(&entityConfigs).Error
	return entityConfigs, err
}

// ReadTenantLevelConfigs reads the latest tenant-level alert configuration based on updated_at timestamp
func ReadTenantLevelConfigs(db *gorm.DB, alertType string, tenantId uuid.UUID) ([]EntityAlertsConfig, error) {
	var tenantConfigs []EntityAlertsConfig
	err := db.Where("tenant_id = ? AND entity_type = ? AND alert_type = ?", tenantId, TenantEntityType, alertType).
		Order("updated_at DESC").
		Limit(1).
		Find(&tenantConfigs).Error
	return tenantConfigs, err
}

// ShouldSkipSandboxAlerts checks if sandbox alerts should be skipped for a given tenant
// Returns (shouldSkip bool, error)
// If there's a database error, returns (true, error) - fail safe by skipping alerts and logging error
// If config exists and enabled=true, returns (true, nil)
// If config doesn't exist or enabled=false, returns (false, nil)
// Results are cached per tenant to avoid repeated database queries
func ShouldSkipSandboxAlerts(db *gorm.DB, tenantId uuid.UUID) (bool, error) {
	tenantIdStr := tenantId.String()

	// Check cache first
	if cached, ok := sandboxAlertCache.Load(tenantIdStr); ok {
		entry := cached.(sandboxAlertCacheEntry)
		logger.GetLogger().Debug("using cached sandbox alerts config",
			zap.String("tenantId", tenantIdStr),
			zap.Bool("shouldSkip", entry.ShouldSkip),
			zap.Bool("hasError", entry.Error != nil))
		return entry.ShouldSkip, entry.Error
	}

	// Not in cache, query database
	configs, err := ReadTenantLevelConfigs(db, "DISABLE_SANDBOX_ALERTS", tenantId)
	if err != nil {
		// If there's an error reading config, skip alerts (fail safe) and return the error for logging
		cacheEntry := sandboxAlertCacheEntry{
			ShouldSkip: true,
			Error:      fmt.Errorf("error reading DISABLE_SANDBOX_ALERTS config: %w", err),
		}
		sandboxAlertCache.Store(tenantIdStr, cacheEntry)
		return cacheEntry.ShouldSkip, cacheEntry.Error
	}
	logger.GetLogger().Info("loaded sandbox alerts config from database",
		zap.String("tenantId", tenantIdStr),
		zap.Any("config", configs))

	var shouldSkip bool
	if len(configs) == 0 {
		// No config found - don't skip sandbox alerts
		shouldSkip = false
	} else {
		config := configs[0]
		if config.Config == nil {
			// Config is nil - don't skip sandbox alerts
			shouldSkip = false
		} else {
			// Skip sandbox alerts if the config is enabled
			shouldSkip = config.Config.Enabled
		}
	}

	// Store result in cache
	cacheEntry := sandboxAlertCacheEntry{
		ShouldSkip: shouldSkip,
		Error:      nil,
	}
	sandboxAlertCache.Store(tenantIdStr, cacheEntry)
	logger.GetLogger().Debug("cached sandbox alerts config",
		zap.String("tenantId", tenantIdStr),
		zap.Bool("shouldSkip", shouldSkip))

	return shouldSkip, nil
}
