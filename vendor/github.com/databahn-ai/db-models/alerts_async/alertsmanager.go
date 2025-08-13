package alerts_async

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/cache"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/common-utils/queue"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

/*
AlertsManager is used for sending alerts.
Internally it sends alerts to a deduplication queue, which batches them and sends them to Kafka.
Because it holds alerts in memory, it is critical to close the manager when service is shutting down.
Close is a blocking call that waits for all alerts to be sent before returning.
*/
type AlertsManager struct {
	dq              *queue.DedupeQueue[*Alert]
	producer        *kafka.Producer
	deDupeDuration  time.Duration
	deDupeQueueSize int
	deDupeMaxSize   int
	closeOnce       sync.Once
	cache           *cache.Cache[Alert]
}

var defaultDeDupeSeconds = utils.GetEnvInt("ALERTS_DEDUPE_DURATION_SECONDS", 60)
var defaultDeDupeDuration = time.Duration(defaultDeDupeSeconds) * time.Second
var defaultDeDupeQueueSize = utils.GetEnvInt("ALERTS_DEDUPE_QUEUE_SIZE", 50)
var defaultDeDupeMaxSize = utils.GetEnvInt("ALERTS_DEDUPE_MAX_SIZE", 50)

type AlertManagerOption func(*AlertsManager)

func WithDeDupeDuration(duration time.Duration) AlertManagerOption {
	return func(a *AlertsManager) {
		a.deDupeDuration = duration
	}
}

func WithDeDupeQueueSize(size int) AlertManagerOption {
	return func(a *AlertsManager) {
		a.deDupeQueueSize = size
	}
}

func WithDeDupeMaxSize(size int) AlertManagerOption {
	return func(a *AlertsManager) {
		a.deDupeMaxSize = size
	}
}

func WithCacheEnabled(ttl time.Duration, maxCachedItems int, cleanupDuration time.Duration) AlertManagerOption {
	return func(a *AlertsManager) {
		a.cache = cache.NewCache[Alert](ttl, maxCachedItems, cache.WithCleanupInterval[Alert](cleanupDuration))
	}
}

func NewAlertsManager(ctx context.Context, configReader configuration.ConfigReader, options ...AlertManagerOption) (*AlertsManager, error) {
	inputClusterBrokers := configReader.GetString(configuration.InputKafkaClusterBootstrapServers)
	conf := map[string]any{
		"acks":             1,
		"linger.ms":        50,
		"compression.type": "gzip",
	}
	alertsCluster := kafka.NewKafkaCluster("alerts-cluster", inputClusterBrokers)
	config := kafka.ProducerConfig{
		Name:       "alerts-producer",
		Topic:      "db.management.alerts",
		ExtraParam: conf,
	}
	kafkaProducer, err := alertsCluster.NewProducer(ctx, config)
	if err != nil {
		return nil, err
	}
	manager := &AlertsManager{
		producer:        kafkaProducer,
		deDupeDuration:  defaultDeDupeDuration,
		deDupeQueueSize: defaultDeDupeQueueSize,
		deDupeMaxSize:   defaultDeDupeMaxSize,
	}
	for _, option := range options {
		option(manager)
	}
	adq, err := queue.NewDedupeQueue[*Alert](
		queue.WithMaxUniqueItems[*Alert](manager.deDupeMaxSize),
		queue.WithMaxBatchDuration[*Alert](manager.deDupeDuration),
		queue.WithInputBuffSize[*Alert](manager.deDupeQueueSize),
		queue.WithKeyFunc(func(alert *Alert) string {
			return alert.Id
		}),
	)
	if err != nil {
		return nil, err
	}
	manager.dq = adq
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
		a.producer.SendAsync(msg, func(err error) {
			logger.GetLogger().Error("error while sending alert to kafka", zap.Error(err))
		})
	}
}

func (a *AlertsManager) processAlerts() {
	a.dq.OnOutput(a.sendAlerts)
}

func (a *AlertsManager) RecordAlertTry(alert *Alert) bool {
	if alert == nil {
		logger.GetLogger().Error("alert is nil, ignoring")
		return false
	}
	if !a.dq.PushTry(alert) {
		logger.GetLogger().Warn("alert queue is full, alert not recorded",
			zap.String("title", alert.Title),
			zap.String("tenantId", alert.TenantId),
			zap.String("entityId", alert.FunctionalityEntityId))
		return false
	}
	return true
}

func (a *AlertsManager) RecordAlertTryWithCacheingAndKey(cacheKey string, alertBuilder func() (*Alert, error)) bool {
	if a.cache == nil {
		logger.GetLogger().Error("cache is not enabled, cannot record alert with cache")
		return false
	}
	if _, ok := a.cache.GetCachedItem(cacheKey); ok {
		logger.GetLogger().Debug("Alert already cached, skipping duplicate", zap.String("key", cacheKey))
		return false
	}
	alert, err := alertBuilder()
	if err != nil {
		logger.GetLogger().Error("failed to create alert", zap.Error(err))
		return false
	}
	if a.RecordAlertTry(alert) {
		a.cache.CacheItem(cacheKey, *alert)
		logger.GetLogger().Debug("Alert recorded and cached", zap.String("key", cacheKey), zap.String("alertId", alert.Id))
		return true
	} else {
		logger.GetLogger().Warn("Failed to record alert, queue is full", zap.String("key", cacheKey), zap.String("alertId", alert.Id))
		return false
	}
}

func (a *AlertsManager) RecordAlertTryWithCacheing(entityId, tenantId, reason, errorMessage string, alertBuilder func() (*Alert, error)) bool {
	cacheKey := GenerateCacheKey(entityId, tenantId, reason, errorMessage)
	return a.RecordAlertTryWithCacheingAndKey(cacheKey, alertBuilder)
}

func (a *AlertsManager) RecordAlert(alert *Alert) {
	if alert == nil {
		logger.GetLogger().Error("alert is nil, ignoring")
		return
	}
	a.dq.Push(alert)
}

func (a *AlertsManager) Close(ctx context.Context) {
	a.closeOnce.Do(func() {
		a.dq.Close()
		a.producer.Close(ctx)
		if a.cache != nil {
			a.cache.Close()
		}
	})
}

// GenerateCacheKey Helper function to generate a simple cache key from important parameters
func GenerateCacheKey(entityId, tenantId, reason, err string) string {
	const maxErrorLength = 30

	if len(err) <= maxErrorLength {
		// For short error messages, use direct concatenation (faster)
		return fmt.Sprintf("%s|%s|%s|%s", entityId, tenantId, reason, err)
	}

	// For long error messages, use hash to keep key length manageable
	hash := sha256.Sum256([]byte(err))
	return fmt.Sprintf("%s|%s|%s|%s", entityId, tenantId, reason, hex.EncodeToString(hash[:8]))
}
