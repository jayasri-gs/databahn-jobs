package common

const INSIGHTS_AGGREGATION = "insights_aggregation"
const DATA_REPLAY = "data-replay"
const DEVICE_INVENTORY_HEALTH = "device_inventory_health"
const ACK_PROCESSOR = "ack_processor"

const FLEET_HEALTH_CHECKER = "fleet-health-checker"
const LOG_SOURCE_ACTIVITY_CHECKER = "log-source-activity-checker"
const LOG_SOURCE_REPUTATION_CHECKER = "log-source-reputation-checker"
const KAFKA_QUERY = "kafka_query"

const EVENT_SEQUENCING = "event-sequencing"
const InfoAlert = "info"
const WarningAlert = "warning"
const SevereAlert = "severe"

const (
	SILENT         = 0
	WHISPERING     = 1
	NOISY          = 2
	StatusInactive = 4
)

const ControllerBaseUrl = "https://controller.dev.databahn.app"
