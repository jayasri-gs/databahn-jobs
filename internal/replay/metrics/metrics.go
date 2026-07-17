package metrics

import (
	"context"
	"sync"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const otelMetricsTopic = "db.metrics.staging.otel"

const (
	MetricReplayAttempted = "total_events_replay_attempted"
	MetricReplaySucceeded = "total_events_replay_succeeded"
	MetricReplayFailed    = "total_events_replay_failed"
)

var (
	metricsProducer *kafka.Producer
	initOnce        sync.Once
	initErr         error
)

func Init(ctx context.Context) error {
	initOnce.Do(func() {
		brokers := config.GetDataReplayConfiguration().GetString(configuration.ProcessKafkaClusterBootstrapServers)
		if brokers == "" {
			initErr = nil
			logger.GetLogger().Warn("replay metrics: kafka brokers not configured, recovery counters disabled")
			return
		}

		cluster := kafka.NewKafkaCluster("replay_metrics_kafka_cluster", brokers)
		prodExtraParam := map[string]any{
			"acks":             1,
			"linger.ms":        1000,
			"batch.size":       100,
			"compression.type": "snappy",
		}
		producer, err := cluster.NewProducer(ctx, kafka.ProducerConfig{
			Name:       "replay_metrics_kafka_producer",
			Topic:      otelMetricsTopic,
			ExtraParam: prodExtraParam,
		})
		if err != nil {
			initErr = err
			return
		}
		metricsProducer = producer
	})
	return initErr
}

func RecordCounter(metricName string, tags map[string]string, value int64) {
	if metricsProducer == nil || value == 0 {
		return
	}

	payload, err := buildCounterPayload(metricName, tags, value)
	if err != nil {
		logger.GetLogger().Error("replay metrics: failed to build payload", zap.Error(err), zap.String("metric", metricName))
		return
	}

	metricsProducer.SendAsync(kafka.Message{
		Message: payload,
	}, func(err error) {
		if err != nil {
			logger.GetLogger().Error("replay metrics: failed to publish", zap.Error(err), zap.String("metric", metricName))
		}
	})
}

func Shutdown(ctx context.Context) {
	if metricsProducer == nil {
		return
	}
	metricsProducer.Close(ctx)
	metricsProducer = nil
}
