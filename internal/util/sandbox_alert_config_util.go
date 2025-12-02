package util

import (
	"sync"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// sandboxAlertCache holds cached results of ShouldSkipSandboxAlerts per tenant
// Key: tenant UUID string, Value: sandboxAlertCacheEntry
var sandboxAlertCache sync.Map

// sandboxAlertCacheInitOnce ensures the cache is initialized only once
var sandboxAlertCacheInitOnce sync.Once

// sandboxAlertCacheInitError stores any error that occurred during cache initialization
var sandboxAlertCacheInitError error

// sandboxAlertCacheEntry represents a cached entry for sandbox alert configuration
type sandboxAlertCacheEntry struct {
	ShouldSkip bool
	Error      error
}

// initializeSandboxAlertCache loads all tenant-level sandbox alert configurations into cache
// This is called once on the first call to ShouldSkipSandboxAlerts
func initializeSandboxAlertCache(db *gorm.DB) {
	logger.GetLogger().Info("initializing sandbox alerts cache for all tenants")

	// Query all tenant-level DISABLE_SANDBOX_ALERTS configurations
	var allConfigs []entities.EntityAlertsConfig
	err := db.Where("entity_type = ? AND alert_type = ?", entities.TenantEntityType, "DISABLE_SANDBOX_ALERTS").
		Order("tenant_id, updated_at DESC").
		Find(&allConfigs).Error

	if err != nil {
		sandboxAlertCacheInitError = err
		logger.GetLogger().Error("error loading all sandbox alert configs, will use fail-safe behavior",
			zap.Error(err))
		return
	}

	// Group by tenant and keep only the latest config per tenant (already ordered by updated_at DESC)
	processedTenants := make(map[string]bool)
	cachedCount := 0

	for _, config := range allConfigs {
		tenantIdStr := config.TenantID.String()

		// Skip if we've already processed this tenant (we want the latest one due to ORDER BY)
		if processedTenants[tenantIdStr] {
			continue
		}
		processedTenants[tenantIdStr] = true

		// Determine shouldSkip value
		var shouldSkip bool
		if config.Config != nil && config.Config.Enabled {
			shouldSkip = true
		} else {
			shouldSkip = false
		}

		// Store in cache
		cacheEntry := sandboxAlertCacheEntry{
			ShouldSkip: shouldSkip,
			Error:      nil,
		}
		sandboxAlertCache.Store(tenantIdStr, cacheEntry)
		cachedCount++

		logger.GetLogger().Debug("cached sandbox alert config for tenant",
			zap.String("tenantId", tenantIdStr),
			zap.Bool("shouldSkip", shouldSkip))
	}

	logger.GetLogger().Info("sandbox alerts cache initialized",
		zap.Int("tenantCount", cachedCount),
		zap.Int("totalConfigs", len(allConfigs)))
}

// ShouldSkipSandboxAlerts checks if sandbox alerts should be skipped for a given tenant
// Returns (shouldSkip bool, error)
// If there's a database error, returns (true, error) - fail safe by skipping alerts and logging error
// If config exists and enabled=true, returns (true, nil)
// If config doesn't exist or enabled=false, returns (false, nil)
// On first call, loads all tenant configurations into cache. Subsequent calls use cached data.
func ShouldSkipSandboxAlerts(db *gorm.DB, tenantId uuid.UUID) (bool, error) {
	// Initialize cache on first call (thread-safe, only happens once)
	sandboxAlertCacheInitOnce.Do(func() {
		initializeSandboxAlertCache(db)
	})

	tenantIdStr := tenantId.String()

	// Check cache
	if cached, ok := sandboxAlertCache.Load(tenantIdStr); ok {
		entry := cached.(sandboxAlertCacheEntry)
		logger.GetLogger().Debug("using cached sandbox alerts config",
			zap.String("tenantId", tenantIdStr),
			zap.Bool("shouldSkip", entry.ShouldSkip),
			zap.Bool("hasError", entry.Error != nil))
		return entry.ShouldSkip, entry.Error
	}

	// Not in cache - check if cache initialization failed
	if sandboxAlertCacheInitError != nil {
		// Cache initialization failed due to database error
		// Fail-safe: skip alerts and return the error
		logger.GetLogger().Error("cache initialization failed, using fail-safe behavior to skip sandbox alerts",
			zap.String("tenantId", tenantIdStr),
			zap.Error(sandboxAlertCacheInitError))
		return true, sandboxAlertCacheInitError
	}

	// Cache initialized successfully but tenant not found
	// This means it's a new tenant or tenant has no config
	// Default to not skipping sandbox alerts
	logger.GetLogger().Debug("tenant not found in sandbox alerts cache, defaulting to not skip",
		zap.String("tenantId", tenantIdStr))

	cacheEntry := sandboxAlertCacheEntry{
		ShouldSkip: false,
		Error:      nil,
	}
	sandboxAlertCache.Store(tenantIdStr, cacheEntry)

	return false, nil
}
