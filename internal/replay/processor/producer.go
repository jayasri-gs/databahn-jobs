package processor

import (
	"context"
	"github.com/databahn-ai/common-utils/ack"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var Producer map[string]*kafka.Producer
var AckProducer *ack.AckProducer

func GetProducer(reqId string, topic string) *kafka.Producer {
	if Producer[topic] == nil {
		logger.GetLogger().Info("producer not yet initialed. calling init process", zap.String("traceId", reqId), zap.Int("thread ", -1))
		InitProducer(reqId, topic)
	}
	return Producer[topic]
}

func InitProducer(reqId string, topic string) {
	Producer = make(map[string]*kafka.Producer)
	logger.GetLogger().Info("initialising producer.", zap.String("traceId", reqId), zap.Int("thread ", -1))
	InputBrokers := utils.GetEnvOrDefault(constants.KafkaInputBootstrapServers, "") //common.GetAppConfiguration().GetString(configuration.KafkaBootstrapServers)
	kafka.NewKafkaCluster(constants.ClusterName, InputBrokers)
	cluster, _ := kafka.GetKafkaCluster(constants.ClusterName)
	prodExtraParam := map[string]any{
		"acks":             1,
		"linger.ms":        1000,
		"batch.size":       1000000,
		"compression.type": "snappy",
		//TODO NEED TO DISCUSS 		//"queue.buffering.max.kbytes": 1048576,
	}
	producer, err := cluster.NewProducer(context.Background(), kafka.ProducerConfig{
		Name:       constants.DataReplayProducer,
		ExtraParam: prodExtraParam,
	})
	if err != nil {
		logger.GetLogger().Error("error while initialising  data producer ", zap.String("traceId", reqId), zap.Int("thread ", -1), zap.String("error", err.Error()))
		return
	}
	Producer[topic] = producer

	processingBrokers := utils.GetEnvOrDefault(constants.KafkaBootstrapServers, "") //common.GetAppConfiguration().GetString(configuration.KafkaBootstrapServers)
	AckProducer, err = ack.NewAckProducer(context.Background(), processingBrokers)
	if err != nil {
		logger.GetLogger().Error("error while initialising status ack producer ", zap.String("traceId", reqId), zap.Int("thread ", -1), zap.String("error", err.Error()))
		return
	}
	logger.GetLogger().Info("initialising complete.", zap.String("traceId", reqId), zap.Int("thread ", -1))
}
