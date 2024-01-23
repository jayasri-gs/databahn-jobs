package common

const INSIGHTS_AGGREGATION = "insights-aggregation"
const DATA_REPLAY = "data-replay"
const INSIGHTS_INTERVAL_MINUTES = 120
const INSIGHTS_STAGING_INDEX_PREFIX = "db_staging_insights_"
const INSIGHTS_READ_BATCH = 500
const INSIGHTS_STORE_INDEX_PREFIX = "db_insights_"

const EDGE_HEALTH_CHECKER = "health-checker"
const LOG_SOURCE_ACTIVITY_CHECKER = "log-source-activity-checker"
const LOG_SOURCE_REPUTATION_CHECKER = "log-source-reputation-checker"

const InfoAlert = "info"
const WarningAlert = "warning"
const SevereAlert = "severe"

const LogSourceStatusActive = "3"

const (
	SILENT     = 1
	NOISY      = 2
	WHISPERING = 3
)
