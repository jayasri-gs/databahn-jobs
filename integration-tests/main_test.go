//go:build integration

package integrationtests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
	"github.com/databahn-ai/pramaan-go/pramaan"
	"gorm.io/gorm"
)

const (
	EmailNotificationTopic    = "db.management.notification.email"
	OpsgenieNotificationTopic = "db.management.notification.opsgenie"
	AlertIndexingTopic        = "db.indexing.alerts"

	jobDockerContext = ".."
)

var (
	jobTest  *pramaan.JobPramaan
	gormTest *gorm.DB
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	tg := pramaan.NewDbTestLogger("TestMain")

	jobTest = pramaan.NewJobMainPramaanBuilder(tg).
		WithKafka().
		WithKafkaTopics([]pramaan.TopicDetails{
			{Topic: EmailNotificationTopic, Partitions: 1},
			{Topic: OpsgenieNotificationTopic, Partitions: 1},
			{Topic: AlertIndexingTopic, Partitions: 1},
		}).
		WithKafkaTopicsToCollect([]string{
			EmailNotificationTopic,
			OpsgenieNotificationTopic,
			AlertIndexingTopic,
		}).
		WithJobDetails("databahn-jobs", jobDockerContext).
		ForControlPlane(true, true).
		WithConfigModifiers([]pramaan.ConfigModifier{
			pramaan.WithObjectStoreS3("us-east-1"),
		}).
		Build(ctx)

	ensureAlertIndices(ctx, jobTest.GetOpenSearch(&testing.T{}))
	initDatabase(ctx, jobTest)

	os.Exit(m.Run())
}

func JobPramaan() *pramaan.JobPramaan {
	return jobTest
}

func GormDB() *gorm.DB {
	return gormTest
}

func initDatabase(ctx context.Context, job *pramaan.JobPramaan) {
	postgres := job.GetPostgres(&testing.T{})
	if err := fixtures.Bootstrap(ctx, postgres.GetDB()); err != nil {
		panic(fmt.Sprintf("bootstrap fixtures: %v", err))
	}

	gormDB, err := fixtures.OpenGorm(postgres.GetDB())
	if err != nil {
		panic(fmt.Sprintf("open gorm: %v", err))
	}
	if err := fixtures.SeedDatabahnTenant(ctx, gormDB); err != nil {
		panic(fmt.Sprintf("seed databahn tenant: %v", err))
	}
	gormTest = gormDB
}

func ensureAlertIndices(ctx context.Context, openSearch *pramaan.OpenSearchPramaan) {
	for _, indexName := range []string{"db_alerts", "db_alerts_internal"} {
		if err := openSearch.CreateIndex(ctx, indexName, nil); err != nil {
			if !strings.Contains(err.Error(), "resource_already_exists_exception") {
				panic(fmt.Sprintf("failed to create index %q: %v", indexName, err))
			}
		}
	}
}
