package notification

import (
	"github.com/databahn-ai/db-models/alerts_async"
)

type NotificationModule interface {
	String() string
	privateNotificationType()
	SupportsAlertFunctionality(functionality alerts_async.Functionality) bool
}

type notificationModule struct {
	value                string
	alertFunctionalities []alerts_async.Functionality
}

func (n notificationModule) String() string {
	return n.value
}

func (n notificationModule) privateNotificationType() {
}

func (n notificationModule) SupportsAlertFunctionality(functionality alerts_async.Functionality) bool {
	for _, af := range n.alertFunctionalities {
		if af.String() == functionality.String() {
			return true
		}
	}
	return false
}

var BackendModule = notificationModule{
	value: "BACKEND",
}

var FleetModule = notificationModule{
	value: "FLEET",
	alertFunctionalities: []alerts_async.Functionality{
		alerts_async.Fleet,
	},
}

var FleetNodeModule = notificationModule{
	value: "FLEET_NODE",
	alertFunctionalities: []alerts_async.Functionality{
		alerts_async.FleetNode,
	},
}

var LogSourceModule = notificationModule{
	value: "LOG_SOURCE",
	alertFunctionalities: []alerts_async.Functionality{
		alerts_async.LogSource,
	},
}

var CloudLogSourceModule = notificationModule{
	value: "CLOUD_LOG_SOURCE",
	alertFunctionalities: []alerts_async.Functionality{
		alerts_async.CloudLogSource,
	},
}

var DestinationModule = notificationModule{
	value: "DESTINATION",
	alertFunctionalities: []alerts_async.Functionality{
		alerts_async.Dispenser,
	},
}

var DailyDigestModule = notificationModule{
	value: "DAILY_DIGEST",
}

var WeeklyDigestModule = notificationModule{
	value: "WEEKLY_DIGEST",
}

var ConnectorModule = notificationModule{
	value: "CONNECTOR",
}

func GetAllModules() []NotificationModule {
	return []NotificationModule{
		BackendModule,
		FleetModule,
		FleetNodeModule,
		LogSourceModule,
		CloudLogSourceModule,
		DestinationModule,
		DailyDigestModule,
		WeeklyDigestModule,
		ConnectorModule,
	}
}

func ParseNotificationModule(module string) NotificationModule {
	for _, m := range GetAllModules() {
		if m.String() == module {
			return m
		}
	}
	return nil
}
