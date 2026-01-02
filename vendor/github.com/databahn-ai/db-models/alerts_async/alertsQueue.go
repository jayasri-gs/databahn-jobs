package alerts_async

import (
	"time"

	"github.com/databahn-ai/common-utils/queue"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

var alertQueue *queue.DedupeQueue[alerts_common.Alert]

func LoadAlertQueue() {
	var err error
	alertQueue, err = queue.NewDedupeQueue[alerts_common.Alert](
		queue.WithMaxUniqueItems[alerts_common.Alert](100),
		queue.WithMaxBatchDuration[alerts_common.Alert](1*time.Minute),
		queue.WithInputBuffSize[alerts_common.Alert](20),
		queue.WithKeyFunc(func(alert alerts_common.Alert) string {
			return alert.Functionality + alert.FunctionalityType + alert.FunctionalityEntityId + alert.Title + alert.Message
		}),
	)
	if err != nil {
		logger.GetLogger().Error("failed to create alert queue", zap.Error(err))
		logger.GetLogger().Panic(err.Error())
	}
	go handleAlerts(alertQueue)
}

func handleAlerts(alertQueue *queue.DedupeQueue[alerts_common.Alert]) {
	alertQueue.OnOutput(func(alerts []alerts_common.Alert) {
		err := PublishAlertsToKafka(alerts, "aws")
		if err != nil {
			logger.GetLogger().Error("failed to send alerts to kafka", zap.Error(err))
		}
	})
}

func RecordAlert(entityId uuid.UUID, entityTenantId uuid.UUID, entityName string, functionalityName, functionalityType, criticality, title, message string) {
	if alertQueue == nil {
		LoadAlertQueue()
	}
	newAlert := alerts_common.Alert{
		ID:                      uuid.New(),
		Criticality:             criticality,
		Title:                   title,
		Message:                 message,
		TenantUUID:              entityTenantId,
		Functionality:           functionalityName,
		FunctionalityEntityId:   entityId.String(),
		FunctionalityEntityName: entityName,
		FunctionalityType:       functionalityType,
		Dismissed:               false,
		FirstObservedAt:         time.Now(),
		LastObservedAt:          time.Now(),
	}
	alertQueue.Push(newAlert)
}

// Deprecated: This is deprecated and will be removed soon.
func RecordAlertV2(entityId uuid.UUID, entityTenantId uuid.UUID, dataplaneId uuid.UUID, entityName string, functionalityName,
	functionalityType, criticality, title, message string, alertType string, errorMessage string, errorCode string) {
	if alertQueue == nil {
		LoadAlertQueue()
	}
	newAlert := alerts_common.Alert{
		ID:                      uuid.New(),
		Criticality:             criticality,
		Title:                   title,
		Message:                 message,
		TenantUUID:              entityTenantId,
		Functionality:           functionalityName,
		FunctionalityEntityId:   entityId.String(),
		FunctionalityEntityName: entityName,
		FunctionalityType:       functionalityType,
		Dismissed:               false,
		FirstObservedAt:         time.Now(),
		LastObservedAt:          time.Now(),
		AlertType:               alertType,
		ErrorMessage:            errorMessage,
		ErrorCode:               errorCode,
		DataPlaneId:             dataplaneId,
	}
	alertQueue.Push(newAlert)
}

func GetAlertQueue() *queue.DedupeQueue[alerts_common.Alert] {
	if alertQueue == nil {
		LoadAlertQueue()
	}
	return alertQueue
}
