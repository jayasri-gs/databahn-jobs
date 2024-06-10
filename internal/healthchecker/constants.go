package healthchecker

const (
	FleetHealthCheckTime           = 5
	AgentHealthCheckTime           = 10
	LogSourceActivityCheckerTime   = 15
	ReputationCheckerTime          = 6
	ReputationCheckerTimeThreshold = 24
	NotificationCluster            = "notification_cluster"
	NotificationProducer           = "notification_service_producer"
	NotificationTopic              = "db.management.notification"

	StatusDisabled = "DISABLED"
	StatusDeleted  = "DELETED"
)
