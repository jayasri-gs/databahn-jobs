package databases

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/databahn-ai/common-utils/configuration"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ManagedDB manages a database connection with automatic credential refresh on auth errors
type ManagedDB struct {
	db             *gorm.DB
	mu             sync.RWMutex
	refreshMu      sync.Mutex // serializes refresh operations to prevent concurrent refreshes
	appConfig      configuration.ConfigReader
	useTablePrefix bool
}

// NewManagedDB creates a new managed database connection.
// GetDB() checks connection health and refreshes credentials only on auth errors.
func NewManagedDB(ctx context.Context, appConfig configuration.ConfigReader, useTablePrefix bool) (*ManagedDB, error) {
	mdb := &ManagedDB{
		appConfig:      appConfig,
		useTablePrefix: useTablePrefix,
	}

	// Initial connection
	if err := mdb.refresh(ctx); err != nil {
		return nil, err
	}

	return mdb, nil
}

// refresh fetches credentials and establishes a new database connection.
// Uses refreshMu to serialize concurrent refresh attempts, preventing races
// where one goroutine closes a connection just established by another.
func (mdb *ManagedDB) refresh(ctx context.Context) error {
	mdb.refreshMu.Lock()
	defer mdb.refreshMu.Unlock()

	// Double-check: if connection is now healthy after acquiring refreshMu,
	// another goroutine just refreshed successfully - skip refresh
	if err := mdb.checkHealth(); err == nil {
		logging.GetLogger().Debug("Connection already healthy after acquiring refresh lock, skipping refresh")
		return nil
	}

	conn := &Connection{
		Host:         mdb.appConfig.GetString(configuration.DatabaseHost),
		Port:         mdb.appConfig.GetString(configuration.DatabasePort),
		SchemaName:   mdb.appConfig.GetString(configuration.DatabaseSchema),
		DatabaseName: mdb.appConfig.GetString(configuration.DatabaseName),
		SSLMode:      mdb.appConfig.GetString(configuration.DatabaseSSLMode),
	}

	db, err := conn.ConnectWithSecrets(ctx, mdb.useTablePrefix, mdb.appConfig)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("failed to connect/reconnect to db", zap.Error(err))
		return err
	}

	// Swap the connection
	mdb.mu.Lock()
	oldDB := mdb.db
	mdb.db = db
	mdb.mu.Unlock()

	// Close old connection if exists.
	// Safe because refreshMu is held - no other refresh can capture this connection as oldDB
	if oldDB != nil {
		if sqlDB, err := oldDB.DB(); err == nil {
			sqlDB.Close()
		}
	}

	logging.GetLogger().Info("Database connection established/refreshed")
	return nil
}

// isAuthError checks if the error is an authentication/authorization error
func isAuthError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// PostgreSQL auth error patterns
	authPatterns := []string{
		"password authentication failed",
		"authentication failed",
		"invalid password",
	}

	for _, pattern := range authPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// checkHealth checks connection health and returns any error
func (mdb *ManagedDB) checkHealth() error {
	mdb.mu.RLock()
	db := mdb.db
	mdb.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("database connection is nil")
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	if err := sqlDB.Ping(); err != nil {
		return err
	}

	return nil
}

// GetDB returns the database connection after checking health.
// If auth error, refreshes credentials and reconnects.
// For other errors, returns the existing connection (caller should handle errors).
func (mdb *ManagedDB) GetDB(ctx context.Context) (*gorm.DB, error) {
	err := mdb.checkHealth()
	if err != nil {
		if isAuthError(err) {
			logging.GetLoggerWithContext(ctx).Warn("Authentication error detected, refreshing credentials",
				zap.Error(err))

			if refreshErr := mdb.refresh(ctx); refreshErr != nil {
				logging.GetLoggerWithContext(ctx).Error("Failed to refresh database connection",
					zap.Error(refreshErr))
				return nil, fmt.Errorf("auth error and refresh failed: %w", refreshErr)
			}

			mdb.mu.RLock()
			defer mdb.mu.RUnlock()
			return mdb.db, nil
		}

		// Non-auth error - return existing connection with error (caller should handle)
		mdb.mu.RLock()
		defer mdb.mu.RUnlock()
		return mdb.db, fmt.Errorf("database health check failed: %w", err)
	}

	mdb.mu.RLock()
	defer mdb.mu.RUnlock()
	return mdb.db, nil
}

// Close closes the database connection
func (mdb *ManagedDB) Close() error {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()

	if mdb.db != nil {
		if sqlDB, err := mdb.db.DB(); err == nil {
			return sqlDB.Close()
		}
	}
	return nil
}
