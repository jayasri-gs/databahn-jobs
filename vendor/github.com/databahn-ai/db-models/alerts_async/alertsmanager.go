package alerts_async

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/common-utils/queue"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"time"
)

/*
AlertsManager is used for sending alerts.
Internally it sends alerts to a deduplication queue, which batches them and sends them to Kafka.
Because it holds alerts in memory, it is critical to close the manager when service is shutting down.
Close is a blocking call that waits for all alerts to be sent before returning.
*/
type AlertsManager struct {
	dq       *queue.DedupeQueue[*Alert]
	producer *kafka.Producer
}

func NewAlertsManager(ctx context.Context, cluster *kafka.Cluster) (*AlertsManager, error) {
	adq, err := queue.NewDedupeQueue[*Alert](
		queue.WithMaxUniqueItems[*Alert](100),
		queue.WithMaxBatchDuration[*Alert](1*time.Minute),
		queue.WithInputBuffSize[*Alert](20),
		queue.WithKeyFunc(func(alert *Alert) string {
			return alert.Functionality + alert.FunctionalityType + alert.FunctionalityEntityId + alert.Title + alert.Message
		}),
	)
	if err != nil {
		return nil, err
	}
	conf := map[string]any{
		"acks":             1,
		"linger.ms":        50,
		"compression.type": "gzip",
	}
	config := kafka.ProducerConfig{
		Name:       "alerts-producer",
		Topic:      "db.management.alerts",
		ExtraParam: conf,
	}
	producer, err = cluster.NewProducer(ctx, config)
	if err != nil {
		return nil, err
	}
	manager := &AlertsManager{
		dq:       adq,
		producer: producer,
	}
	go manager.processAlerts()
	return manager, nil
}

func (a *AlertsManager) sendAlerts(alerts []*Alert) {
	for _, alert := range alerts {
		alertBytes, _ := json.Marshal(alert)
		msg := kafka.Message{
			Key:     []byte(alert.Id),
			Message: alertBytes,
			Headers: nil,
		}
		producer.SendAsync(msg, func(err error) {
			logger.GetLogger().Error("error while sending alert to kafka", zap.Error(err))
		})
	}
}

func (a *AlertsManager) processAlerts() {
	a.dq.OnOutput(a.sendAlerts)
}

func (a *AlertsManager) RecordAlert(alert *Alert) {
	if alert == nil {
		logger.GetLogger().Error("alert is nil, ignoring")
		return
	}
	a.dq.Push(alert)
}

func (a *AlertsManager) Close(ctx context.Context) {
	a.dq.Close()
	a.producer.Close(ctx)
}
