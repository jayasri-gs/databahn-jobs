//go:build integration

package integrationtests

import (
	"context"
	"testing"

	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
	"github.com/google/uuid"
)

func TestBootstrapModulesAndNotificationFixture(t *testing.T) {
	fixture, err := fixtures.SeedNotificationFixture(context.Background(), GormDB(), fixtures.DefaultNotificationFixtureOptions())
	if err != nil {
		t.Fatalf("seed notification fixture: %v", err)
	}

	if fixture.TenantID == uuid.Nil {
		t.Fatal("expected tenant id to be set")
	}
	if len(fixtures.LiquibaseModules) == 0 {
		t.Fatal("expected liquibase modules catalog to be populated")
	}
}
