package fixtures

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"sync"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

//go:embed schema.sql
var schemaSQL string

var (
	bootstrapOnce sync.Once
	bootstrapErr  error
)

// Bootstrap applies schema and seeds modules once per test process.
func Bootstrap(ctx context.Context, db *sql.DB) error {
	bootstrapOnce.Do(func() {
		bootstrapErr = applySchema(ctx, db)
		if bootstrapErr != nil {
			return
		}
		bootstrapErr = SeedModules(ctx, db)
	})
	return bootstrapErr
}

func applySchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply fixtures schema: %w", err)
	}
	return nil
}

// OpenGorm opens GORM with the same naming strategy as databahn services.
func OpenGorm(db *sql.DB) (*gorm.DB, error) {
	return gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{SingularTable: true},
	})
}

// SeedModules inserts the Liquibase module catalog (idempotent).
func SeedModules(ctx context.Context, db *sql.DB) error {
	gormDB, err := OpenGorm(db)
	if err != nil {
		return err
	}

	rows := make([]entities.Modules, 0, len(LiquibaseModules))
	for _, module := range LiquibaseModules {
		moduleID, err := uuid.Parse(module.ID)
		if err != nil {
			return fmt.Errorf("parse module id %q: %w", module.ID, err)
		}
		rows = append(rows, entities.Modules{
			ID:          moduleID,
			Name:        module.Name,
			Description: module.Description,
		})
	}

	return gormDB.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{"name", "description"}),
		}).
		Create(&rows).Error
}

// ModuleIDByName returns a module UUID from the seeded catalog.
func ModuleIDByName(name string) (uuid.UUID, error) {
	for _, module := range LiquibaseModules {
		if module.Name == name {
			return uuid.Parse(module.ID)
		}
	}
	return uuid.Nil, fmt.Errorf("module %q not found in liquibase catalog", name)
}
