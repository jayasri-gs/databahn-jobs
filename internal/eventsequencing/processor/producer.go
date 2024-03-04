package processor

import (
	"context"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/constants"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var Producer *kafka.Producer

func GetProducer(reqId string) *kafka.Producer {
	if Producer == nil {
		logger.GetLogger().Info("producer not yet initialed. calling init process", zap.String("traceId", reqId), zap.Int("thread ", -1))
		InitProducer(reqId)
	}
	return Producer
}

func InitProducer(reqId string) {

	logger.GetLogger().Info("initialising producer.", zap.String("traceId", reqId), zap.Int("thread ", -1))

	boostrap := config.GetAppConfiguration().GetString(configuration.KafkaBootstrapServers)
	kafka.NewKafkaCluster(constants.ClusterName, boostrap)
	cluster, _ := kafka.GetKafkaCluster(constants.ClusterName)
	prodExtraParam := map[string]any{
		"acks":             1,
		"linger.ms":        1000,
		"batch.size":       1000000,
		"compression.type": "snappy",
		//TODO NEED TO DISCUSS 		//"queue.buffering.max.kbytes": 1048576,
	}
	producer, err := cluster.NewProducer(context.Background(), kafka.ProducerConfig{
		Name:       constants.DataSequenceProducer,
		ExtraParam: prodExtraParam,
	})

	if err != nil {

		logger.GetLogger().Error("error while initialising  data producer ", zap.String("traceId", reqId), zap.Int("thread ", -1), zap.String("error", err.Error()))
		return
	}
	Producer = producer
	logger.GetLogger().Info("initialising complete.", zap.String("traceId", reqId), zap.Int("thread ", -1))
}
