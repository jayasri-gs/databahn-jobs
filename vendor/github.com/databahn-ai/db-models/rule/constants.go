package rule

const (
	DB_AGGREGATE_COUNT   = "db_aggregation_count"
	DB_EVENT_ID          = "db_event_id"
	DB_WINDOW_START_TIME = "db_window_start_time"
	DB_WINDOW_END_TIME   = "db_window_end_time"
)

const (
	MINUTE_INTERVAL = "MINUTES"
	HOUR_INTERVAL   = "HOURS"
	DAY_INTERVAL    = "DAYS"
)

const (
	SAMPLING_TYPE        = "Sampling"
	SUPPRESSION_TYPE     = "Suppression"
	AGGREGATION_TYPE     = "Data Aggregation"
	SAMPLING_TOPIC       = "db.staging.sampling"
	SUPPRESSION_TOPIC    = "db.staging.suppression"
	AGGREGATION_TOPIC    = "db.staging.aggregation"
	AGGREGATION_V2_TOPIC = "db.staging.aggregation.v2"
	SANDBOX_TOPIC        = "db.destination.sandbox"
	TRANSFORM_TOPIC      = "db.staging.transform"
)

const (
	ModelV1 = "V1"
)
