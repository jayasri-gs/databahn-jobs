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
	SILENT     = "SILENT"
	STABLE     = "STABLE"
	WHISPERING = "WHISPERING"
	NOISY      = "NOISY"
)

const StatusCreated = "CREATED"

const NoisyThreshold = 1.5
const ControllerBaseUrl = "https://controller.dev.databahn.app"

const AlertsIndex = "db_alerts"

const AgentHealthCheck = "agent health check"
const AgentFunctionality = "endpoint health checker"
const FleetNodeHealthCheckTitle = "node health not reported"
const AgentHealthCheckTitle = "unhealthy agent nodes found"
const FleetNodeHealthCheckMessage = "node health not reported for more than 5 minutes"
const AgentHealthCheckMessage = "health from agent not reported for more than 5 minutes, marking unhealthy"

const FleetConnectorHealthCheckMessage = "connector health not reported for more than 5 minutes"

const FleetConnectorHealthCheckTitle = "fleet connector health check"

const ALERT_REPORT_PROCESSOR = "alert_report_processor"
