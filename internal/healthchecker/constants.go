package healthchecker

import (
	"github.com/databahn-ai/common-utils/utils"
	"time"
)

const (
	EnvFleetHealthCheckTime           = "FLEET_HEALTH_CHECK_TIME"
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

var FleetHealthCheckTime = time.Duration(utils.GetEnvInt(EnvFleetHealthCheckTime, 30))
var FleetHealthCheckIgnoreTime = time.Duration(utils.GetEnvInt(EnvFleetHealthCheckIgnoreTime, 600))
var LogSourceActivityCheckerTime = time.Duration(utils.GetEnvInt(EnvLogSourceActivityCheckerTime, 30))
var ReputationCheckerTime = time.Duration(utils.GetEnvInt(EnvReputationCheckerTime, 6))
var ReputationCheckerTimeThreshold = time.Duration(utils.GetEnvInt(EnvReputationCheckerTimeThreshold, 24))
