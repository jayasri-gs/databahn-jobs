package ack

import (
	"context"
	"encoding/json"

	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type AckProducer struct {
	producer *kafka.Producer
}

func NewAckProducer(ctx context.Context, brokers string) (*AckProducer, error) {
	cluster := kafka.NewKafkaCluster("acknowledgements", brokers)
	prodExtraParam := map[string]any{
		"acks":             1,
		"linger.ms":        5,
		"compression.type": "snappy",
	}

	c := kafka.ProducerConfig{
		Name:       "acknowledgements",
		Topic:      constants.ChangeFlagAckTopic,
		ExtraParam: prodExtraParam,
	}
	producer, err := cluster.NewProducer(ctx, c)
	if err != nil {
		logger.GetLogger().Error("failed to create producer", zap.Error(err))
		return nil, err
	}
	return &AckProducer{producer: producer}, nil
}

func (p *AckProducer) Produce(ctx context.Context, ack Ack, headers []kafka.Header) error {
	// Route to appropriate topic based on playground flag
	topic := constants.ChangeFlagAckTopic
	if ack.IsPlayground {
		topic = constants.ChangeFlagAckPlaygroundTopic
		logger.GetLogger().Debug("routing to playground ack topic", zap.String("entityId", ack.EntityId), zap.String("topic", topic))
	}

	ackBytes, err := json.Marshal(ack)
	if err != nil {
		logger.GetLogger().Error("failed to marshal ack", zap.Error(err))
		return err
	}
	msg := kafka.Message{
		Message: ackBytes,
		Headers: headers,
	}
	err = p.producer.SendSyncTopic(ctx, msg, topic)
	if err != nil {
		logger.GetLogger().Error("failed to produce acknowledgement to kafka", zap.String("topic", topic), zap.Error(err))
		return err
	}
	return nil
}

func (p *AckProducer) Close(ctx context.Context) {
	p.producer.Close(ctx)
}
