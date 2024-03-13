package statistics

const (
	URL_PARAM_QUERY      string = "q"
	URL_PARAM_START_TIME string = "startTime"
	URL_PARAM_END_TIME   string = "endTime"
	URL_PARAM_AGG        string = "agg"
	URL_PARAM_INTERVAL   string = "interval"
)

const (
	ES_COUNTER_VALUE_FIELD string = "counter.value"
	ES_TIME_FIELD          string = "tags.db_ts_win"
	PROCESSING_TIME_FIELD  string = "timestamp"
)

const (
	TAG_TENANT_ID string = "tags.db_tenant_id"
)
