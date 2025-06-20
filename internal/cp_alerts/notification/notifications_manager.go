package notification

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/kafka"
	cn "github.com/databahn-ai/common-utils/notification"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type NotificationManager struct {
	producer *kafka.Producer
}

func NewNotificationManager(ctx context.Context) (*NotificationManager, error) {
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
		ExtraParam: prodExtraParam,
	}
	kafkaProducer, err := cluster.NewProducer(ctx, producerConfig)
	if err != nil {
		return nil, err
	}
	return &NotificationManager{
		producer: kafkaProducer,
	}, nil
}

func (n *NotificationManager) SendOpsGenieNotification(request cn.OpsGenieNotificationRequest) error {
	jsonRequest, err := json.Marshal(request)
	if err != nil {
		return err
	}
	message := kafka.Message{
		Key:     nil,
		Message: jsonRequest,
	}
	n.producer.SendAsyncTopic(message, "db.management.notification.opsgenie", func(err error) {
		logger.GetLogger().Error("error while sending email notification", zap.Error(err))
	})
	logger.GetLogger().Info("opsgenie notification request sent", zap.String("subject", request.Subject))
	return nil
}

func (n *NotificationManager) SendEmailNotification(request cn.EmailNotificationRequest) error {
	jsonRequest, err := json.Marshal(request)
	if err != nil {
		return err
	}
	message := kafka.Message{
		Key:     nil,
		Message: jsonRequest,
	}
	n.producer.SendAsyncTopic(message, "db.management.notification.email", func(err error) {
		logger.GetLogger().Error("error while sending email notification", zap.Error(err))
	})
	logger.GetLogger().Info("email notification request sent", zap.String("title", request.Subject))
	return nil
}

func (n *NotificationManager) Close(ctx context.Context) {
	if n.producer != nil {
		n.producer.Close(ctx)
	}
}
