package healthchecker

import (
	"github.com/databahn-ai/common-utils/utils"
)

const (
	EnvFleetHealthCheckTime           = "FLEET_HEALTH_CHECK_TIME"
	EnvAgentHealthCheckTime           = "AGENT_HEALTH_CHECK_TIME"
	EnvFleetHealthCheckIgnoreTime     = "FLEET_HEALTH_CHECK_IGNORE_TIME"
	EnvLogSourceActivityCheckerTime   = "LOG_SOURCE_ACTIVITY_CHECKER_TIME"
	EnvReputationCheckerTime          = "REPUTATION_CHECKER_TIME"
	EnvReputationCheckerTimeThreshold = "REPUTATION_CHECKER_TIME_THRESHOLD"

	NotificationCluster  = "notification_cluster"
	NotificationProducer = "notification_service_producer"
	NotificationTopic    = "db.management.notification"

	StatusDisabled = "DISABLED"
	StatusDeleted  = "DELETED"
	StatusCreated  = "CREATED"
	StatusInactive = "INACTIVE"
)

var FleetHealthCheckTime = utils.GetEnvOrDefault(EnvFleetHealthCheckTime, "10")
var AgentHealthCheckTime = utils.GetEnvOrDefault(EnvAgentHealthCheckTime, "10")
var FleetHealthCheckIgnoreTime = utils.GetEnvOrDefault(EnvFleetHealthCheckIgnoreTime, "600")
var LogSourceActivityCheckerTime = utils.GetEnvOrDefault(EnvLogSourceActivityCheckerTime, "30")
var DestinationDeliveryCheckerTime = utils.GetEnvOrDefault(EnvLogSourceActivityCheckerTime, "30")
var ReputationCheckerTime = utils.GetEnvOrDefault(EnvReputationCheckerTime, "6")
var ReputationCheckerTimeThreshold = utils.GetEnvOrDefault(EnvReputationCheckerTimeThreshold, "24")
