package insights

import "github.com/databahn-ai/common-utils/utils"

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
