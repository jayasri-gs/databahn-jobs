package alert

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type AlertsManager struct {
	producer *kafka.Producer
}

func NewAlertsManager(ctx context.Context) (*AlertsManager, error) {
	kafkaBrokers := config.GetAppConfiguration().GetString(configuration.KafkaBootstrapServers)
	if kafkaBrokers == "" {
		return nil, errors.New("kafka bootstrap server is not configured")
	}
	cluster := kafka.NewKafkaCluster("cp_cluster", kafkaBrokers)
	prodExtraParam := map[string]any{
		"acks":             1,
		"linger.ms":        1000,
		"batch.size":       1000,
		"compression.type": "snappy",
	}
	producerConfig := kafka.ProducerConfig{
		Name:       "notification_producer",
		Topic:      "db.indexing.alerts",
		ExtraParam: prodExtraParam,
	}
	kafkaProducer, err := cluster.NewProducer(ctx, producerConfig)
	if err != nil {
		return nil, err
	}
	return &AlertsManager{
		producer: kafkaProducer,
	}, nil
}

func (am *AlertsManager) SendAlerts(alerts []*alerts_async.Alert) error {
	if len(alerts) == 0 {
		return nil
	}
	for _, alert := range alerts {
		jsonRequest, err := json.Marshal(alert)
		if err != nil {
			return err
		}
		headers := []kafka.Header{
			{Key: "action", Value: []byte("index")},
		}
		message := kafka.Message{
			Key:     []byte(alert.Id),
			Message: jsonRequest,
			Headers: headers,
		}
		am.producer.SendAsync(message, func(err error) {
			logger.GetLogger().Error("error while sending alert", zap.Error(err))
		})
	}
	return nil
}

func (am *AlertsManager) AutoResolveAlerts(alertIds []string) error {
	if len(alertIds) == 0 {
		return nil
	}
	for _, alertId := range alertIds {
		body := map[string]any{
			"id":        alertId,
			"status":    alerts_async.AlertAutoResolved.Value(),
			"dismissed": true,
		}
		if err := am.sendAlertUpdate(alertId, body, "status_update"); err != nil {
			return err
		}
	}
	return nil
}

// NotificationSentUpdate records notification state written back to OpenSearch.
type NotificationSentUpdate struct {
	NotificationCount    int
	LastNotificationTime int64
	LastActivationTime   int64
}

// RecordNotificationSent updates notificationCount and lastNotificationTime for alerts that were emailed.
// LastActivationTime is included when set, to backfill legacy alerts missing that field.
func (am *AlertsManager) RecordNotificationSent(updates map[string]NotificationSentUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	for alertID, update := range updates {
		body := map[string]any{
			"id":                   alertID,
			"notificationCount":    update.NotificationCount,
			"lastNotificationTime": update.LastNotificationTime,
		}
		if update.LastActivationTime > 0 {
			body["lastActivationTime"] = update.LastActivationTime
		}
		if err := am.sendAlertUpdate(alertID, body, "notification_sent"); err != nil {
			return err
		}
	}
	return nil
}

func (am *AlertsManager) sendAlertUpdate(alertId string, body map[string]any, action string) error {
	jsonRequest, err := json.Marshal(body)
	if err != nil {
		return err
	}
	message := kafka.Message{
		Key:     []byte(alertId),
		Message: jsonRequest,
		Headers: []kafka.Header{
			{Key: "action", Value: []byte(action)},
		},
	}
	am.producer.SendAsync(message, func(err error) {
		logger.GetLogger().Error("error while sending alert update request", zap.String("action", action), zap.Error(err))
	})
	return nil
}

func (am *AlertsManager) Close(ctx context.Context) {
	am.producer.Close(ctx)
}
