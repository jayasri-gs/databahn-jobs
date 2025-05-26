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

func (am *AlertsManager) DismissAlerts(alertIds []string) error {
	if len(alertIds) == 0 {
		return nil
	}
	for _, alertId := range alertIds {
		body := make(map[string]any)
		body["id"] = alertId
		body["status"] = alerts_async.AlertAutoResolved.Value()
		body["dismissed"] = "true"
		jsonRequest, err := json.Marshal(body)
		if err != nil {
			return err
		}
		headers := []kafka.Header{
			{Key: "action", Value: []byte("status_update")},
		}
		message := kafka.Message{
			Key:     []byte(alertId),
			Message: jsonRequest,
			Headers: headers,
		}
		am.producer.SendAsync(message, func(err error) {
			logger.GetLogger().Error("error while sending dismiss alert request", zap.Error(err))
		})
	}
	return nil
}

func (am *AlertsManager) Close(ctx context.Context) {
	am.producer.Close(ctx)
}
