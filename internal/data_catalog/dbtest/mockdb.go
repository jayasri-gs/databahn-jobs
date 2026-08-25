package dbtest

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// MockPostgres wires a sqlmock-backed GORM DB into config.GetDB for unit tests.
func MockPostgres(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm on sqlmock: %v", err)
	}

	config.SetDBForTest(gormDB)
	t.Cleanup(func() {
		config.SetDBForTest(nil)
		_ = sqlDB.Close()
	})

	return gormDB, mock
}
