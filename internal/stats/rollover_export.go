package stats

import (
	"context"
	"fmt"
	"time"

	"github.com/opensearch-project/opensearch-go/v2"
)

// NewIndexNameForRollover builds the destination index name for a rollover bucket size.
func (in Index) NewIndexNameForRollover(duration time.Duration) (string, error) {
	return in.newIndexNameForRollover(duration)
}

// RollOverIndex aggregates stats from index into newIndexName and validates totals.
// It is exported for integration tests that exercise the p1_p2 / p2_p3 / p3_p4 rollover path.
func RollOverIndex(ctx context.Context, index Index, client *opensearch.Client, config *RolloverConfig, newIndexName string) error {
	return doRolloverAndValidate(ctx, index, client, config, newIndexName)
}

// NewIntegrationRolloverConfig returns a RolloverConfig aligned with lifecycle jobs (hourly or daily buckets).
func NewIntegrationRolloverConfig(bucket time.Duration) (*RolloverConfig, error) {
	if bucket <= 0 {
		return nil, fmt.Errorf("integration rollover bucket must be positive, got %s", bucket)
	}
	return &RolloverConfig{
		aggBatchSize:                  500,
		aggQueryRange:                 bucket,
		validationRange:               bucket,
		aggWindow:                     bucket,
		deleteExistingRolledOverIndex: false,
		skipValidation:                false,
		skipTenantIdValidation:        false,
	}, nil
}
