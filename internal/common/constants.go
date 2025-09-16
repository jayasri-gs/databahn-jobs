package common

const INSIGHTS_AGGREGATION = "insights_aggregation"
const ROLLOVER_OLDER_STATS = "rollover_older_stats"
const STATS_LIFECYCLE = "stats_lifecycle"
const DATA_REPLAY = "data-replay"
const DEVICE_INVENTORY_HEALTH = "device_inventory_health"
const ACK_PROCESSOR = "ack_processor"
const VC_ALERTS = "vc-alerts"

const FLEET_HEALTH_CHECKER = "fleet-health-checker"
const LOG_SOURCE_ACTIVITY_CHECKER = "log-source-activity-checker"
const TRANSFORMATION_VALIDATOR = "transformation-validator"

// TO BE deleted post agent fix
const FHL_WINDOWS_ACTIVITY_CHECKER = "fhl-windows-activity-checker"
const LOG_SOURCE_ACTIVITY_CHECKER_NEW = "log-source-activity-checker-new"
const DESTINATION_ACTIVITY_CHECKER = "destination-activity-checker"
const DESTINATION_MORE_THAN_INJECTED = "destination-delivered-more-than-injected"
const NOTIFICATIONS_FOR_ALERTS = "notifications-for-alerts"
const LOG_SOURCE_REPUTATION_CHECKER = "log-source-reputation-checker"
const ENTITY_CHECKER_ALERT_GEN_V2 = "entity-reputation-checker-alert-gen-v2"
const SILENT_DEVICE_ALERT = "silent-device-alert"
const DEVICE_INVENTORY_ALERT = "device-inventory-alert"
const TENANT_DAILY_DIGEST = "tenant-daily-digest"
const HEALTH_CHECKER = "health-checker"
const KAFKA_QUERY = "kafka_query"
const UNPARSED_EVENTS = "unparsed-events"
const EVENT_SEQUENCING = "event-sequencing"
const VOLUME_DEVIATION_ALERT = "volume-deviation-alert"
const VC_NO_REDUCTION_ALERT = "vc-no-reduction-alert"
const DEPLOYMENT_DELAY_ALERT = "deployment-delay-alert"
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
const AlertsIndexInternal = "db_alerts_internal"
const DatabahnTenantId = "dbd00000-0000-0000-0000-000000000000"
const DatabahnDataPlaneId = "dbd00000-0000-0000-0000-000000000000"

const FleetNodeHealthCheck = "fleet health check"
const FleetNodeHealthCheckTitle = "No heartbeat received in the last 30 minutes"
const FleetNodeHealthCheckMessage = "This alert indicates that the cloud has not received a heartbeat signal from the specified fleet node for the past 30 minutes. Heartbeat signals are periodic messages sent by fleet node to confirm that they are running and operational. The absence of these signals suggests a potential issue that may require immediate attention."

const AgentHealthCheck = "agent health check"
const AgentFunctionality = "endpoint health checker"
const AgentHealthCheckerFunctionalityTitle = "Health from agent not reported for more than %s, marking unhealthy for agent %s"
const FleetNodeHealthCheckerFunctionalityTitle = "Health from fleet_node not reported for more than %s, marking unhealthy for fleet node %s"
const FleetConnectorHealthCheckerFunctionalityTitle = "Health from fleet_connector not reported for more than %s, marking unhealthy for fleet connector %s"
const AgentHealthCheckTitle = "Unhealthy agent nodes found"
const AgentHealthCheckMessage = "Health from agent not reported for more than 5 minutes, marking unhealthy"

// New message constants with time information
const AgentHealthCheckerFunctionalityMessage = "This agent is configured to be alerted when health is not reported for more than %s. As of '%s' last heartbeat was received at '%s'."
const FleetNodeHealthCheckerFunctionalityMessage = "This fleet node is configured to be alerted when health is not reported for more than %s. As of '%s' last heartbeat was received at '%s'."
const FleetConnectorHealthCheckerFunctionalityMessage = "This fleet connector is configured to be alerted when health is not reported for more than %s. As of '%s' last heartbeat was received at '%s'."
const FleetComponentHealthCheckerFunctionalityMessage = "This fleet component is configured to be alerted when health is not reported for more than %s. As of '%s' last heartbeat was received at '%s'."

const FleetConnectorHealthCheckMessage = "This alert indicates that the cloud has not received a heartbeat signal from the specified connector for the past 30 minutes. Heartbeat signals are periodic messages sent by connector to confirm that they are running and operational. The absence of these signals suggests a potential issue that may require immediate attention."
const FleetComponentHealthCheckMessage = "This alert indicates that the cloud has not received a heartbeat signal from the specified component for the past 30 minutes. Heartbeat signals are periodic messages sent by connector to confirm that they are running and operational. The absence of these signals suggests a potential issue that may require immediate attention."

const FleetConnectorHealthCheckTitle = "No heartbeat received in the last 30 minutes"
const FleetComponentHealthCheckTitle = "Fleet component %s is not reporting health for more than %s"

const ALERT_REPORT_PROCESSOR = "alert_report_processor"

const DATA_HEALTH_SCORE_JOB = "data_health_score_job"
const UNPARSED_EVENTS_REPORT = "unparsed-events-report"
