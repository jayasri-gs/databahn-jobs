package insights

const INSIGHTS_INTERVAL_MINUTES = 120
const INSIGHTS_STAGING_INDEX_PREFIX = "db_staging_insights_"
const INSIGHTS_READ_BATCH = 500
const INSIGHTS_STORE_INDEX_PREFIX = "db_insights_"

// app names should not have underscores
const APP_DEVICEINVENTORY = "deviceinventory"

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

const NOISE_ZSCORE_THRESHOLD = 3

const STATUS_ERROR = "error"
const STATUS_SUCCESS = "success"
