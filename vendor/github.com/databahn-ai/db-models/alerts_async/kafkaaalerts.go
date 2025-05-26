package alerts_async

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"time"
)

var producer *kafka.Producer

func getProducer(ctx context.Context, clusterName string) error {
	if producer == nil || producer.Producer.IsClosed() {
		cluster, err := kafka.GetKafkaCluster(clusterName)
		if err != nil {
			return err
		}
		config := kafka.ProducerConfig{
			Name:  "alerts-producer",
			Topic: "db.management.alerts",
		}
		producer, err = cluster.NewProducer(ctx, config)
		if err != nil {
			return err
		}
	}
	return nil
}

func CloseAlertsProducer(ctx context.Context) {
	if producer != nil {
		producer.Close(ctx)
	}
}

// Deprecated: This is deprecated and will be removed soon.
func SendAlertToKafka(ctx context.Context, entities []alerts_common.AlertEntityObject, clusterName, functionalityName, functionalityType, criticality, title, message string) error {
	err := getProducer(ctx, clusterName)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while getting producer", zap.Error(err))
		return err
	}
	for _, e := range entities {
		alertBytes, _ := json.Marshal(getAlertObject(e, criticality, title, message, functionalityName, functionalityType))
		msg := kafka.Message{
			Key:     nil,
			Message: alertBytes,
			Headers: nil,
		}
		producer.SendAsync(msg, func(err error) {
			logger.GetLogger().Error("error while sending message", zap.Error(err))
		})
	}
	return nil
}

// Deprecated: This is deprecated and will be removed soon.
func PublishAlertsToKafka(alerts []alerts_common.Alert, clusterName string) error {
	ctx := context.Background()
	err := getProducer(ctx, clusterName)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while getting producer", zap.Error(err))
		return err
	}
	for _, alert := range alerts {
		alertBytes, _ := json.Marshal(alert)
		msg := kafka.Message{
			Key:     nil,
			Message: alertBytes,
			Headers: nil,
		}
		producer.SendAsync(msg, func(err error) {
			logger.GetLogger().Error("error while sending message", zap.Error(err))
		})
	}
	return nil
}
func getAlertObject(entity alerts_common.AlertEntityObject, criticality, title, message, functionalityName, functionalityType string) alerts_common.Alert {
	newAlert := alerts_common.Alert{
		ID:                      uuid.New(),
		Criticality:             criticality,
		Title:                   title,
		Message:                 message,
		TenantUUID:              entity.EntityTenantUUId,
		Functionality:           functionalityName,
		FunctionalityEntityId:   entity.EntityId.String(),
		FunctionalityEntityName: entity.EntityName,
		FunctionalityType:       functionalityType,
		Dismissed:               false,
		FirstObservedAt:         time.Now(),
		LastObservedAt:          time.Now(),
	}
	return newAlert
}
