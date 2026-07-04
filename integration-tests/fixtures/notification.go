package fixtures

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SeedDatabahnTenant inserts the shared Databahn engineering tenant used by internal alert processing.
func SeedDatabahnTenant(ctx context.Context, db *gorm.DB) error {
	tenantID, err := uuid.Parse(common.DatabahnTenantId)
	if err != nil {
		return fmt.Errorf("parse databahn tenant id: %w", err)
	}

	tenant := map[string]any{
		"id":     tenantID,
		"name":   "Databahn Engineering",
		"active": true,
	}
	if err := db.WithContext(ctx).Table("tenants").
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoNothing: true}).
		Create(tenant).Error; err != nil {
		return fmt.Errorf("insert databahn tenant: %w", err)
	}
	return nil
}

// NotificationFixture holds IDs created for notification integration tests.
type NotificationFixture struct {
	TenantID uuid.UUID
	TargetID uuid.UUID
	ActorID  uuid.UUID
}

// NotificationFixtureOptions configures tenant/module/target seed data.
type NotificationFixtureOptions struct {
	TenantName  string
	TargetName  string
	TargetEmail string
	ModuleNames []string
	// ModuleTenantConfig is applied to every enabled module mapping when set.
	ModuleTenantConfig *entities.ModuleTenantConfigData
}

// DefaultNotificationFixtureOptions returns sensible defaults for alert notification tests.
func DefaultNotificationFixtureOptions() NotificationFixtureOptions {
	return NotificationFixtureOptions{
		TenantName:  "integration-test-tenant",
		TargetName:  "integration-email-target",
		TargetEmail: "integration-test@databahn.ai",
		ModuleNames: []string{"LOG_SOURCE", "AGENT", "DESTINATION"},
	}
}

// SeedNotificationFixture creates a new tenant (fresh UUID), targets, module mappings,
// and module_targets rows. Call once per test case that needs isolated notification state.
func SeedNotificationFixture(ctx context.Context, db *gorm.DB, opts NotificationFixtureOptions) (*NotificationFixture, error) {
	if opts.TenantName == "" {
		opts.TenantName = "integration-test-tenant"
	}
	if opts.TargetName == "" {
		opts.TargetName = "integration-email-target"
	}
	if opts.TargetEmail == "" {
		opts.TargetEmail = "integration-test@databahn.ai"
	}
	if len(opts.ModuleNames) == 0 {
		opts.ModuleNames = []string{"LOG_SOURCE"}
	}

	tenantID := uuid.New()
	targetID := uuid.New()
	actorID := uuid.New()
	now := time.Now().UTC()

	targetConfig, err := json.Marshal(map[string]string{
		"to": opts.TargetEmail,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal target configuration: %w", err)
	}

	tenant := map[string]any{
		"id":     tenantID,
		"name":   opts.TenantName,
		"active": true,
	}
	target := entities.Targets{
		ID:            targetID,
		Name:          opts.TargetName,
		Description:   "integration test email target",
		Type:          "EMAIL",
		Configuration: datatypes.JSON(targetConfig),
		TenantID:      tenantID,
		CreatedBy:     actorID,
		UpdatedBy:     actorID,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := db.WithContext(ctx).Table("tenants").Create(tenant).Error; err != nil {
		return nil, fmt.Errorf("insert tenant: %w", err)
	}
	if err := db.WithContext(ctx).Create(&target).Error; err != nil {
		return nil, fmt.Errorf("insert target: %w", err)
	}

	for _, moduleName := range opts.ModuleNames {
		moduleID, err := ModuleIDByName(moduleName)
		if err != nil {
			return nil, err
		}

		mapping := entities.ModuleTenantMapping{
			ID:       uuid.New(),
			TenantID: tenantID,
			ModuleID: moduleID,
			Enabled:  true,
			Config:   "",
		}
		if opts.ModuleTenantConfig != nil {
			mapping.ModuleTenantConfig = opts.ModuleTenantConfig
		}

		if err := db.WithContext(ctx).Table("module_tenant_mapping").Create(&mapping).Error; err != nil {
			return nil, fmt.Errorf("insert module_tenant_mapping for %s: %w", moduleName, err)
		}

		moduleTarget := entities.ModuleTargets{
			ID:        uuid.New(),
			ModuleID:  moduleID,
			TargetID:  targetID,
			CreatedBy: actorID,
			CreatedAt: now,
		}
		if err := db.WithContext(ctx).Table("module_targets").Create(&moduleTarget).Error; err != nil {
			return nil, fmt.Errorf("insert module_targets for %s: %w", moduleName, err)
		}
	}

	return &NotificationFixture{
		TenantID: tenantID,
		TargetID: targetID,
		ActorID:  actorID,
	}, nil
}
