package constants

const (
	ServiceName    = "SERVICE_NAME"
	ServiceVersion = "SERVICE_VERSION"

	SourceScopeFleet         = "FLEET"
	SourceScopeDataGenerator = "DATA_GENERATOR"
	SourceScopeCloud         = "CLOUD"
	OtelComponentName        = "component_name"
	OtelSourceScope          = "scope"
	DbTimestampWindow        = "db_ts_win"
	OtelServiceName          = "service_name"
)

const (
	DedupEventsAllTenantId          = "00000000-0000-0000-0000-000000000000"
	DedupEventsAllowedFPRate        = "DEDUP_EVENTS_ALLOWED_FP_RATE"
	DedupEventsTTLWindow            = "DEDUP_EVENTS_TTL_WINDOW"
	DedupEventsBloomExpectedEntries = 26000000 //This is assuming 500 events per second and approximately 15 hours rotation window
	DedupEventsDefaultAllowedFPRate = 0.05
	Connector                       = "connector"
	Source                          = "source"
	DedupEventsRedisTTLBloomSeconds = 40 * 60 * 60 //40 hours max ttl for blooms
	ConnectorKey                    = "connector_key"
	ErrorCount                      = "error_count"
	BloomOccupancyRatio             = "bloom_occupancy_ratio"
	BloomFalsePositiveRatio         = "bloom_false_positive_ratio"
	DedupTime                       = "dedup_time"
	OriginalEventsCount             = "original_events_count"
	UniqueEventsCount               = "unique_events_count"
)
