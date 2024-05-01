package helper

import (
	"context"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var Producer *kafka.Producer

func GetProducer() *kafka.Producer {
	if Producer == nil {
		logger.GetLogger().Info("producer not yet initialed. calling init process", zap.String("topic", healthchecker.NotificationTopic))
		InitProducer()
	}
	return Producer
}

func InitProducer() {

	logger.GetLogger().Info("initialising producer.", zap.String("topic", healthchecker.NotificationTopic))

	boostrap := config.GetAppConfiguration().GetString(configuration.KafkaBootstrapServers)
	kafka.NewKafkaCluster(healthchecker.NotificationCluster, boostrap)
	cluster, _ := kafka.GetKafkaCluster(healthchecker.NotificationCluster)
	prodExtraParam := map[string]any{
		"acks":             1,
		"linger.ms":        1000,
		"batch.size":       1000,
		"compression.type": "snappy",
		//TODO NEED TO DISCUSS 		//"queue.buffering.max.kbytes": 1048576,
	}
	producer, err := cluster.NewProducer(context.Background(), kafka.ProducerConfig{
		Name:       healthchecker.NotificationProducer,
		ExtraParam: prodExtraParam,
	})

	if err != nil {

		logger.GetLogger().Error("error while initialising  notification alert producer ", zap.String("topic", healthchecker.NotificationTopic), zap.String("error", err.Error()))
		return
	}
	Producer = producer
	logger.GetLogger().Info("initialising complete.", zap.String("topic", healthchecker.NotificationTopic))
}
