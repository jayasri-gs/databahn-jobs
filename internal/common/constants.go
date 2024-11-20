package common

const INSIGHTS_AGGREGATION = "insights_aggregation"
const ROLLOVER_OLDER_STATS = "rollover_older_stats"
const DATA_REPLAY = "data-replay"
const DEVICE_INVENTORY_HEALTH = "device_inventory_health"
const ACK_PROCESSOR = "ack_processor"

const FLEET_HEALTH_CHECKER = "fleet-health-checker"
const LOG_SOURCE_ACTIVITY_CHECKER = "log-source-activity-checker"
const LOG_SOURCE_REPUTATION_CHECKER = "log-source-reputation-checker"
const TENANT_DAILY_DIGEST = "tenant-daily-digest"
const AGENT_HEALTH_CHECKER = "agent-health-checker"
const KAFKA_QUERY = "kafka_query"
const UNPARSED_EVENTS = "unparsed-events"
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

const FleetNodeHealthCheck = "fleet health check"
const FleetNodeHealthCheckTitle = "No heartbeat received in the last 30 minutes"
const FleetNodeHealthCheckMessage = "This alert indicates that the cloud has not received a heartbeat signal from the specified fleet node for the past 30 minutes. Heartbeat signals are periodic messages sent by fleet node to confirm that they are running and operational. The absence of these signals suggests a potential issue that may require immediate attention."

const AgentHealthCheck = "agent health check"
const AgentFunctionality = "endpoint health checker"
const AgentHealthCheckTitle = "unhealthy agent nodes found"
const AgentHealthCheckMessage = "health from agent not reported for more than 5 minutes, marking unhealthy"

const FleetConnectorHealthCheckMessage = "This alert indicates that the cloud has not received a heartbeat signal from the specified connector for the past 30 minutes. Heartbeat signals are periodic messages sent by connector to confirm that they are running and operational. The absence of these signals suggests a potential issue that may require immediate attention."

const FleetConnectorHealthCheckTitle = "No heartbeat received in the last 30 minutes"

const ALERT_REPORT_PROCESSOR = "alert_report_processor"
