package processor

import (
	"context"
	"strings"

	"github.com/databahn-ai/common-utils/ack"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

var Producer map[string]*kafka.Producer
var AckProducer *ack.AckProducer

func matchReplayType(replayType, expected string) bool {
	return strings.EqualFold(strings.TrimSpace(replayType), expected)
}

func replayUsesInputKafka(replayType string) bool {
	if strings.TrimSpace(replayType) == "" {
		return true
	}
	return matchReplayType(replayType, "CUSTOM")
}

func replayDataBrokers(replayType string) string {
	if replayUsesInputKafka(replayType) {
		return utils.GetEnvOrDefault(constants.KafkaInputBootstrapServers, "")
	}
	return utils.GetEnvOrDefault(constants.KafkaProcessingBootstrapServers, "")
}

func GetProducer(reqId string, topic string) *kafka.Producer {
	if Producer == nil || Producer[topic] == nil {
		logger.GetLogger().Info("producer not yet initialed. calling init process", zap.String("traceId", reqId), zap.Int("thread ", -1))
		InitProducer(reqId, topic, "")
	}
	return Producer[topic]
}

func InitProducer(reqId string, topic string, replayType string) {
	Producer = make(map[string]*kafka.Producer)
	dataBrokers := replayDataBrokers(replayType)
	clusterKind := "processing"
	if replayUsesInputKafka(replayType) {
		clusterKind = "input"
	}
	logger.GetLogger().Info("initialising replay data producer",
		zap.String("traceId", reqId),
		zap.String("replayType", replayType),
		zap.String("kafkaCluster", clusterKind),
		zap.String("brokers", dataBrokers))

	kafka.NewKafkaCluster(constants.ClusterName, dataBrokers)
	cluster, _ := kafka.GetKafkaCluster(constants.ClusterName)
	prodExtraParam := map[string]any{
		"acks":             1,
		"linger.ms":        1000,
		"batch.size":       1000000,
		"compression.type": "snappy",
	}
	producer, err := cluster.NewProducer(context.Background(), kafka.ProducerConfig{
		Name:       constants.DataReplayProducer,
		ExtraParam: prodExtraParam,
	})
	if err != nil {
		logger.GetLogger().Error("error while initialising data producer",
			zap.String("traceId", reqId),
			zap.String("error", err.Error()))
		return
	}
	Producer[topic] = producer

	inputBrokers := utils.GetEnvOrDefault(constants.KafkaInputBootstrapServers, "")
	AckProducer, err = ack.NewAckProducer(context.Background(), inputBrokers)
	if err != nil {
		logger.GetLogger().Error("error while initialising status ack producer",
			zap.String("traceId", reqId),
			zap.String("error", err.Error()))
		return
	}
	logger.GetLogger().Info("replay producer initialisation complete", zap.String("traceId", reqId))
}
