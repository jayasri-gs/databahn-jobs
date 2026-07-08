//go:build integration

package integrationtests

import (
	"context"
	"testing"

	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
)

func SeedNewNotificationTenant(t *testing.T, opts fixtures.NotificationFixtureOptions) *fixtures.NotificationFixture {
	t.Helper()

	fixture, err := fixtures.SeedNotificationFixture(context.Background(), GormDB(), opts)
	if err != nil {
		t.Fatalf("seed notification fixture: %v", err)
	}
	return fixture
}
