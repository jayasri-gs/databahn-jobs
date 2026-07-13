package insights

import (
	"strconv"
	"strings"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const INSIGHTS_INTERVAL_MINUTES = 60
const INSIGHTS_STAGING_INDEX_PREFIX = "db_staging_insights_"
const INSIGHTS_STORE_INDEX_PREFIX = "db_insights_"
const defaultInsightsReadBatch = 500
const defaultDeviceAggWriteBatch = 200
const deviceAggWriteBatchEnv = "DEVICE_AGG_WRITE_BATCH"

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

func getDeviceAggWriteBatch() int {
	batch := utils.GetEnvInt(deviceAggWriteBatchEnv, defaultDeviceAggWriteBatch)
	if batch < 1 {
		logger.GetLogger().Warn("invalid device agg write batch, using default",
			zap.String("env", deviceAggWriteBatchEnv),
			zap.Int("value", batch),
			zap.Int("default", defaultDeviceAggWriteBatch))
		return defaultDeviceAggWriteBatch
	}
	return batch
}

const deviceAggEnv = "DEVICE_AGG"

// deviceAggMode is parsed once per job run in loadDeviceAggMode.
var deviceAggMode string

func loadDeviceAggMode() {
	deviceAggMode = parseDeviceAggModeFromEnv()
}

func parseDeviceAggModeFromEnv() string {
	val := strings.ToUpper(strings.TrimSpace(utils.GetEnvOrDefault(deviceAggEnv, "")))
	switch val {
	case "", "BACKFILL", "AGG":
		return val
	default:
		logger.GetLogger().Error("invalid DEVICE_AGG value, treating as disabled", zap.String("value", val))
		return ""
	}
}

func deviceBackfillEnabled() bool {
	return deviceAggMode == "BACKFILL"
}

func deviceAggEnabled() bool {
	return deviceAggMode == "AGG"
}

const insightsTestSkipObjectStoreUploadEnv = "INSIGHTS_TEST_SKIP_OBJECT_STORE_UPLOAD"

// testSkipInsightsObjectStoreUpload skips S3/Blob uploads when INSIGHTS_TEST_SKIP_OBJECT_STORE_UPLOAD is true.
// For integration tests only; never enable in production.
func testSkipInsightsObjectStoreUpload() bool {
	return parseTestSkipInsightsObjectStoreUploadFromEnv()
}

func parseTestSkipInsightsObjectStoreUploadFromEnv() bool {
	raw := strings.TrimSpace(utils.GetEnvOrDefault(insightsTestSkipObjectStoreUploadEnv, "false"))
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		logger.GetLogger().Warn("invalid INSIGHTS_TEST_SKIP_OBJECT_STORE_UPLOAD, defaulting to false",
			zap.String("value", raw), zap.Error(err))
		return false
	}
	return enabled
}
