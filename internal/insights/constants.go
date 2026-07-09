package insights

import (
	"strings"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const INSIGHTS_INTERVAL_MINUTES = 60
const INSIGHTS_STAGING_INDEX_PREFIX = "db_staging_insights_"
const INSIGHTS_STORE_INDEX_PREFIX = "db_insights_"
const defaultInsightsReadBatch = 500

// app names should not have underscores
const APP_TYPE_SOURCEHOSTNAME = "sourcehostname"

const AGG_SIGHTS = "sights"
const AGG_FREQUENCY = "frequency"
const REPUTATION_HISTORY = "reputation_history"

const REPUTATION_NORMAL = "normal"
const REPUTATION_SILENT = "silent"
const REPUTATION_NOISY = "noisy"
const REPUTATION_WHISPERING = "whispering"

const HEALTH_CALCULATION_TODAY = "today"
const HEALTH_CALCULATION_YESTERDAY = "yesterday"

const SilentDaysBefore = 2

const NOISE_DAYS_TO_CONSIDER = 30

const STATUS_ERROR = "error"
const STATUS_SUCCESS = "success"

func getInsightsReadBatch() int {
	return utils.GetEnvInt("INSIGHTS_READ_BATCH", defaultInsightsReadBatch)
}

const deviceAggEnv = "DEVICE_AGG"

func getDeviceAggMode() string {
	val := strings.ToUpper(strings.TrimSpace(utils.GetEnvOrDefault(deviceAggEnv, "")))
	switch val {
	case "", "BACKFILL", "AGG":
		return val
	default:
		logger.GetLogger().Warn("invalid DEVICE_AGG value, treating as disabled", zap.String("value", val))
		return ""
	}
}

func deviceBackfillEnabled() bool {
	return getDeviceAggMode() == "BACKFILL"
}

func deviceAggEnabled() bool {
	return getDeviceAggMode() == "AGG"
}

// testSkipInsightsObjectStoreUpload skips S3/Blob uploads when INSIGHTS_TEST_SKIP_OBJECT_STORE_UPLOAD is set.
// For integration tests only; never enable in production.
func testSkipInsightsObjectStoreUpload() bool {
	return utils.GetEnvOrDefault("INSIGHTS_TEST_SKIP_OBJECT_STORE_UPLOAD", "false") != "false"
}
