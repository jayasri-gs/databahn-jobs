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
	ackBytes, err := json.Marshal(ack)
	if err != nil {
		logger.GetLogger().Error("failed to marshal ack", zap.Error(err))
		return err
	}
	msg := kafka.Message{
		Message: ackBytes,
		Headers: headers,
	}
	err = p.producer.SendSync(ctx, msg)
	if err != nil {
		logger.GetLogger().Error("failed to produce acknowledgement to kafka", zap.Error(err))
		return err
	}
	return nil
}

func (p *AckProducer) Close(ctx context.Context) {
	p.producer.Close(ctx)
}
