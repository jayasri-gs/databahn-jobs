package common

const INSIGHTS_AGGREGATION = "insights_aggregation"
const DATA_REPLAY = "data-replay"
const DEVICE_INVENTORY_HEALTH = "device_inventory_health"

const FLEET_HEALTH_CHECKER = "fleet-health-checker"
const LOG_SOURCE_ACTIVITY_CHECKER = "log-source-activity-checker"
const LOG_SOURCE_REPUTATION_CHECKER = "log-source-reputation-checker"
const KAFKA_QUERY = "kafka_query"

const InfoAlert = "info"
const WarningAlert = "warning"
const SevereAlert = "severe"

const (
	SILENT     = 0
	STABLE     = 1
	WHISPERING = 2
	NOISY      = 3
)

const NoisyThreshold = 1.5
const ControllerBaseUrl = "https://controller.dev.databahn.app"

const WhisperingAlertTitle = "Whispering Log Source"
const WhisperingAlertMessage = "Log source is whispering"
const WhisperingAlertType = "whispering-log-source-alert"
const NoisyAlertTitle = "Noisy Log Source"
const NoisyAlertMessage = "Log source is noisy"
const NoisyAlertType = "noisy-log-source-alert"
