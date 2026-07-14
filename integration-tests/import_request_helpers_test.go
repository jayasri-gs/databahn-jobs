//go:build integration

package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/databahn-ai/databahn-jobs/integration-tests/fixtures"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/insights"
	"github.com/databahn-ai/pramaan-go/pramaan"
	"github.com/google/uuid"
)

const (
	importRequestProcessorJobName = common.IMPORT_REQUEST_PROCESSOR
	importRequestJobTimeout       = 3 * time.Minute
)

type importRequestRow struct {
	ID           uuid.UUID       `gorm:"column:id"`
	Status       string          `gorm:"column:status"`
	ErrorMessage string          `gorm:"column:error_message"`
	Stats        json.RawMessage `gorm:"column:stats"`
	Retries      int             `gorm:"column:retries"`
	CreatedBy    uuid.UUID       `gorm:"column:created_by"`
}

type importRequestStats struct {
	TotalRows     int `json:"total_rows"`
	ProcessedRows int `json:"processed_rows"`
	SkippedRows   int `json:"skipped_rows"`
	FailedRows    int `json:"failed_rows"`
}

type deviceTimezoneCSVRow struct {
	Hostname string
	Timezone string
}

func seedImportRequestFixture(t *testing.T) *fixtures.ImportRequestFixture {
	t.Helper()
	fixture, err := fixtures.SeedImportRequestFixture(context.Background(), GormDB())
	if err != nil {
		t.Fatalf("seed import request fixture: %v", err)
	}
	return fixture
}

func buildDeviceTimezoneCSV(rows []deviceTimezoneCSVRow) []byte {
	var buff strings.Builder
	buff.WriteString("hostname,device_timezone\n")
	for _, row := range rows {
		buff.WriteString(row.Hostname)
		buff.WriteString(",")
		buff.WriteString(row.Timezone)
		buff.WriteByte('\n')
	}
	return []byte(buff.String())
}

func uploadArtifactsObject(
	t *testing.T,
	ctx context.Context,
	cloud *pramaan.CloudPramaan,
	key string,
	content []byte,
) {
	t.Helper()
	client, err := newCloudS3Client(cloud.GetExternalEndpoint(), cloud)
	if err != nil {
		t.Fatalf("create cloud s3 client: %v", err)
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cloud.GetArtifactsBucket()),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		t.Fatalf("upload artifacts object %q: %v", key, err)
	}
}

func newCloudS3Client(endpoint string, cloud *pramaan.CloudPramaan) (*s3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cloud.GetRegion()),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cloud.GetAccessKey(),
				cloud.GetSecretKey(),
				"",
			),
		),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	}), nil
}

func runImportRequestProcessorJob(t *testing.T, ctx context.Context, tg pramaan.TestLogger, job *pramaan.JobPramaan) {
	t.Helper()
	job.Run(ctx, tg, pramaan.JobRunOptions{
		Cmd:     []string{"-job", importRequestProcessorJobName},
		Timeout: importRequestJobTimeout,
	})
}

func getImportRequestRow(t *testing.T, ctx context.Context, id uuid.UUID) importRequestRow {
	t.Helper()
	var row importRequestRow
	if err := GormDB().WithContext(ctx).Table("import_request").Where("id = ?", id).First(&row).Error; err != nil {
		t.Fatalf("load import_request %s: %v", id, err)
	}
	return row
}

func parseImportRequestStats(t *testing.T, raw json.RawMessage) importRequestStats {
	t.Helper()
	if len(raw) == 0 {
		t.Fatalf("import_request stats is empty")
	}
	var stats importRequestStats
	if err := json.Unmarshal(raw, &stats); err != nil {
		t.Fatalf("decode import_request stats: %v", err)
	}
	return stats
}

func cleanupImportRequestTest(
	t *testing.T,
	ctx context.Context,
	openSearch *pramaan.OpenSearchPramaan,
	tenantID string,
	importRequestID uuid.UUID,
) {
	t.Helper()
	_ = GormDB().WithContext(ctx).Table("import_request").Where("id = ?", importRequestID).Delete(nil).Error
	_ = openSearch.DeleteIndex(ctx, devicesIndexName(tenantID))
}

func deviceIDForTenant(tenantID uuid.UUID, host string) string {
	return insights.DeviceId(tenantID.String(), host)
}
