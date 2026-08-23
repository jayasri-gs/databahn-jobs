package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"time"

	"github.com/databahn-ai/common-utils/utils"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type Cluster struct {
	Brokers   string
	Consumers map[string]*Consumer
	Producers map[string]*Producer
}

type Header struct {
	Key   string
	Value []byte
}

type ConsumedMessage[K any, V any] struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       K
	Value     *V
	Headers   []Header
	Timestamp int64
}

type SimpleKafkaConsumer[K any, V any] interface {
	Process(key K, message V) error
	KeyDeserialize(b []byte) (K, error)
	ValueDeserialize(b []byte) (V, error)
}

type DetailedKafkaConsumer[K any, V any] interface {
	Process(message *ConsumedMessage[K, V]) error
	KeyDeserialize(b []byte) (K, error)
	ValueDeserialize(b []byte) (V, error)
}
type ConsumerConfig struct {
	Name                     string
	GroupId                  string
	NoOfThreads              int
	Topics                   []string
	OffsetReset              OffsetReset
	ExtraParam               map[string]any
	PartitionBalanceCallback func(*kafka.Consumer, kafka.Event) error
}

func (c ConsumerConfig) getNumberOfThreads() int {
	threadCount := runtime.NumCPU()
	if c.NoOfThreads != 0 {
		threadCount = c.NoOfThreads
	}
	return threadCount
}

type OffsetReset struct {
	value string
}

func (reset OffsetReset) LibraryValue() string {
	return reset.value
}

var EarliestOffset OffsetReset = OffsetReset{value: "earliest"}
var LatestOffset OffsetReset = OffsetReset{value: "latest"}

type ProducerConfig struct {
	Name       string
	Topic      string
	ExtraParam map[string]any
	// OnDeliveryError, if set, is invoked for every asynchronous delivery failure observed on the
	// producer's Events() channel (per-message topic errors and cluster-level errors). It lets a
	// service surface these failures as a metric/alert instead of them being log-only. Optional and
	// nil-safe; runs on a dedicated dispatcher goroutine (decoupled from the shared events-draining
	// loop), but keep it cheap and non-blocking since that goroutine is single-threaded per producer.
	OnDeliveryError func(topic string, err error)
}

type Producer struct {
	Producer        *kafka.Producer
	Config          ProducerConfig
	deliveryErrChan chan deliveryErrEvent
}

type deliveryErrEvent struct {
	topic string
	err   error
}

// deliveryErrDispatchBuffer bounds how many pending OnDeliveryError callbacks can queue before
// new ones are dropped (and logged) rather than blocking the shared producer events-draining loop.
const deliveryErrDispatchBuffer = 256

type Consumer struct {
	quit   chan struct{}
	Config ConsumerConfig
}

type Message struct {
	Key     []byte
	Message []byte
	Headers []Header
}

var clustersConfigured = make(map[string]*Cluster)

func NewKafkaCluster(name string, brokers string) *Cluster {
	cluster := &Cluster{
		Brokers:   brokers,
		Consumers: make(map[string]*Consumer),
		Producers: make(map[string]*Producer),
	}
	clustersConfigured[name] = cluster
	return cluster
}

// GetKafkaCluster using with name until we use some Dependency Inject tool
func GetKafkaCluster(name string) (*Cluster, error) {
	if cluster, ok := clustersConfigured[name]; ok {
		return cluster, nil
	} else {
		logger.GetLogger().Panic("No cluster configured with name", zap.String("clusterName", name))
		return nil, errors.New("No cluster configured with name " + name)
	}
}

func (c Cluster) NewProducer(ctx context.Context, config ProducerConfig) (*Producer, error) {
	if _, ok := c.Producers[config.Name]; ok {
		logger.GetLoggerWithContext(ctx).Error("Producer already registered", zap.String("producerName", config.Name))
		err := errors.New("Producer already exists " + config.Name)
		return nil, err
	}
	if config.ExtraParam == nil {
		config.ExtraParam = map[string]any{
			"acks":             1,
			"linger.ms":        1000,
			"batch.size":       1000000,
			"compression.type": "snappy",
		}
	} else {
		if _, ok := config.ExtraParam["acks"]; !ok {
			config.ExtraParam["acks"] = 1
		}
		if _, ok := config.ExtraParam["linger.ms"]; !ok {
			config.ExtraParam["linger.ms"] = 1000
		}
		if _, ok := config.ExtraParam["batch.size"]; !ok {
			config.ExtraParam["batch.size"] = 1000000
		}
		if _, ok := config.ExtraParam["compression.type"]; !ok {
			config.ExtraParam["compression.type"] = "snappy"
		}
	}
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": c.Brokers,
	}

	for key, value := range config.ExtraParam {
		configMap.SetKey(key, value)
	}
	CheckAndUpdateSSLConfig(configMap)
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	if producer != nil {
		md, err := producer.GetMetadata(nil, false, 5000)
		if err != nil {
			logger.GetLogger().Error("failed to connect to kafka cluster", zap.String("brokers", c.Brokers), zap.Error(err))
			producer.Close()
			return nil, err
		} else {
			logger.GetLogger().Debug("connected to kafka cluster", zap.String("producerName", config.Name), zap.Int("brokersCount", len(md.Brokers)),
				zap.Int("topicsCount", len(md.Topics)))
		}
	}

	deliveryErrChan := make(chan deliveryErrEvent, deliveryErrDispatchBuffer)
	if config.OnDeliveryError != nil {
		// Dispatch the callback on a dedicated goroutine, decoupled from the shared events-draining
		// loop below, so slow callback logic (logging, alerting, metrics I/O) cannot stall delivery
		// confirmation processing for this producer.
		go func(name string, onErr func(topic string, err error)) {
			for evt := range deliveryErrChan {
				callDeliveryErrCallback(name, onErr, evt)
			}
		}(config.Name, config.OnDeliveryError)
	}

	go func(name string) {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logger.GetLoggerWithContext(ctx).Error("Failed to send to Kafka topic", zap.Error(ev.TopicPartition.Error),
						zap.String("producerName", name), zap.String("Topic", config.Topic))
					if config.OnDeliveryError != nil {
						topic := config.Topic
						if ev.TopicPartition.Topic != nil {
							topic = *ev.TopicPartition.Topic
						}
						dispatchDeliveryErr(deliveryErrChan, topic, ev.TopicPartition.Error, name)
					}
				}
			case kafka.Error:
				logger.GetLoggerWithContext(ctx).Error("Failed to send to Kafka cluster", zap.Error(ev),
					zap.String("producerName", name), zap.String("Topic", config.Topic))
				if config.OnDeliveryError != nil {
					dispatchDeliveryErr(deliveryErrChan, config.Topic, ev, name)
				}
			case *kafka.Stats:
				var stats map[string]interface{}
				err := json.Unmarshal([]byte(e.String()), &stats)
				if err == nil {
					logger.GetLogger().Info("kafka producer stats", zap.String("producerName", name),
						zap.Any("msg_max", stats["msg_max"]), zap.Any("msg_size", stats["msg_size"]), zap.Any("msg_cnt", stats["msg_cnt"]))
				} else {
					logger.GetLogger().Error("failed to parse kafka stats", zap.Error(err))
				}
			}
		}
	}(config.Name)

	return &Producer{
		Producer:        producer,
		Config:          config,
		deliveryErrChan: deliveryErrChan,
	}, nil
}

// dispatchDeliveryErr enqueues a delivery failure for async callback dispatch without blocking the
// caller (the shared producer events-draining loop). If the dispatch queue is full — meaning the
// callback is not keeping up — the notification is dropped and logged rather than backing up event
// draining; the failure itself was already logged synchronously by the caller.
func dispatchDeliveryErr(ch chan deliveryErrEvent, topic string, err error, producerName string) {
	select {
	case ch <- deliveryErrEvent{topic: topic, err: err}:
	default:
		logger.GetLogger().Warn("delivery error callback queue full, dropping notification (error already logged)",
			zap.String("producerName", producerName), zap.String("topic", topic))
	}
}

// callDeliveryErrCallback invokes the caller-supplied OnDeliveryError callback with panic
// protection, mirroring the recover() pattern used around consumer message processing elsewhere
// in this file. The dispatcher goroutine that calls this runs for the lifetime of the producer, so
// an unrecovered panic in caller code would otherwise crash the whole process.
func callDeliveryErrCallback(producerName string, onErr func(topic string, err error), evt deliveryErrEvent) {
	defer func() {
		if r := recover(); r != nil {
			logger.GetLogger().Error("recovered from panic in OnDeliveryError callback",
				zap.String("producerName", producerName), zap.String("topic", evt.topic), zap.Any("recovered", r))
		}
	}()
	onErr(evt.topic, evt.err)
}

func (c Cluster) GetProducer(ctx context.Context, name string) (*Producer, error) {
	if p, ok := c.Producers[name]; ok {
		return p, nil
	}
	logger.GetLoggerWithContext(ctx).Error("Producer is not registered", zap.String("producerName", name))
	err := errors.New("Producer is not registered " + name)
	return nil, err
}

func (p Producer) SendSync(ctx context.Context, message Message) error {
	return p.SendSyncTopic(ctx, message, p.Config.Topic)
}

func (p Producer) SendSyncTopic(ctx context.Context, message Message, topic string) error {
	// Buffered (cap 1) and deliberately never closed: librdkafka delivers exactly one event here
	// for a successfully enqueued message, and if ctx is canceled below before that arrives, this
	// function returns while the delivery is still in flight. A buffered slot lets that late write
	// land without blocking or panicking (unlike an unbuffered channel closed via defer); the
	// channel is then garbage collected once both sides are done with it.
	report := make(chan kafka.Event, 1)
	kafkaMessage := adaptMessage(message, topic)
	err := p.Producer.Produce(kafkaMessage, report)
	if err != nil {
		// A local enqueue failure (e.g. queue full, message too large, producer closed) means the
		// message was never handed to the broker. Surface it so a "sync" send genuinely confirms
		// delivery instead of silently reporting success on a dropped message.
		return err
	}
	select {
	case e := <-report:
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				return ev.TopicPartition.Error
			} else {
				return nil
			}
		case kafka.Error:
			return ev
		default:
			logger.GetLoggerWithContext(ctx).Info("Ignored kafka producer result", zap.String("producerName", p.Config.Name), zap.Any("Result", ev))
			return nil
		}
	case <-ctx.Done():
		// Without this, ctx was accepted but never actually honored: the caller could block here
		// indefinitely (bounded only by librdkafka's own message.timeout.ms) regardless of the
		// caller's own deadline/cancellation.
		return ctx.Err()
	}
}

func (p Producer) SendAsync(message Message, callback func(err error)) {
	p.SendAsyncTopic(message, p.Config.Topic, callback)
}

func (p Producer) SendAsyncTopic(message Message, topic string, callback func(err error)) {
	kafkaMessage := adaptMessage(message, topic)
	err := p.retrySending(kafkaMessage, 0, 1000)
	if err != nil && callback != nil {
		callback(err)
	}
}

func (p Producer) retrySending(message *kafka.Message, retry int, flushTime int) error {
	if retry == 3 {
		return p.Producer.Produce(message, nil)
	}
	err := p.Producer.Produce(message, nil)
	if err == nil {
		return nil
	}
	if kfkErr, ok := err.(kafka.Error); ok && kfkErr.Code() == kafka.ErrQueueFull {
		flushCount := p.Producer.Flush(flushTime)
		logger.GetLogger().Info("kafka local queue full, flushed messages, resending after flush", zap.Int("count", flushCount),
			zap.Int("retry", retry), zap.Int("flushTime", flushTime))
		return p.retrySending(message, retry+1, flushTime+1000)
	} else {
		return err
	}
}

func NewSimpleConsumer[K any, V any](c *Cluster, ctx context.Context, config ConsumerConfig, consumer SimpleKafkaConsumer[K, V]) error {
	if _, ok := c.Consumers[config.Name]; ok {
		logger.GetLoggerWithContext(ctx).Error("Consumer already registered", zap.String("consumerName", config.Name))
		return errors.New("Consumer already exists " + config.Name)
	}

	actualProcessor := func(message *kafka.Message) {
		defer func() {
			if r := recover(); r != nil {
				logger.GetLoggerWithContext(ctx).Error("Recovered from panic during processing message", zap.String("consumerName", config.Name), zap.Any("recovered", r))
			}
		}()
		k, err := consumer.KeyDeserialize(message.Key)
		if err != nil {
			logger.GetLogger().Error("Error while deserializing key", zap.Error(err), zap.String("consumerName", config.Name))
			return
		}
		v, err := consumer.ValueDeserialize(message.Value)
		if err != nil {
			logger.GetLogger().Error("Error while deserializing value", zap.Error(err), zap.String("consumerName", config.Name))
			return
		}
		err = consumer.Process(k, v)
		if err != nil {
			logger.GetLogger().Error("Error while processing message", zap.Error(err), zap.String("consumerName", config.Name))
		}
	}

	err := c.startConsumerThreads(ctx, config, actualProcessor)
	if err != nil {
		return err
	}

	return nil
}

func NewDetailedConsumer[K any, V any](c Cluster, ctx context.Context, config ConsumerConfig, consumer DetailedKafkaConsumer[K, V]) error {
	if _, ok := c.Consumers[config.Name]; ok {
		logger.GetLoggerWithContext(ctx).Error("Consumer already registered", zap.String("consumerName", config.Name))
		return errors.New("Consumer already exists " + config.Name)
	}
	actualProcessor := func(message *kafka.Message) {
		defer func() {
			if r := recover(); r != nil {
				logger.GetLoggerWithContext(ctx).Error("Recovered from panic during processing message", zap.String("consumerName", config.Name), zap.Any("recovered", r))
			}
		}()
		k, err := consumer.KeyDeserialize(message.Key)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("Error while deserializing key", zap.Error(err), zap.String("consumerName", config.Name))
			return
		}
		v, err := consumer.ValueDeserialize(message.Value)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("Error while deserializing value", zap.Error(err), zap.String("consumerName", config.Name))
			return
		}
		detailedMessage := &ConsumedMessage[K, V]{
			Topic:     *message.TopicPartition.Topic,
			Partition: message.TopicPartition.Partition,
			Offset:    int64(message.TopicPartition.Offset),
			Key:       k,
			Value:     &v,
			Headers:   adaptToDBHeaders(message.Headers),
			Timestamp: message.Timestamp.UnixMilli(),
		}
		err = consumer.Process(detailedMessage)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("Error while processing message", zap.Error(err), zap.String("consumerName", config.Name))
		}
	}

	err := c.startConsumerThreads(ctx, config, actualProcessor)
	if err != nil {
		return err
	}

	return nil
}

func (c Cluster) startConsumerThreads(ctx context.Context, config ConsumerConfig, actualProcessor func(message *kafka.Message)) error {
	quit := make(chan struct{})
	if config.ExtraParam == nil {
		config.ExtraParam = map[string]any{
			"partition.assignment.strategy": "roundrobin",
			"fetch.max.bytes":               104857600,
		}
	} else {
		if _, ok := config.ExtraParam["partition.assignment.strategy"]; !ok {
			config.ExtraParam["partition.assignment.strategy"] = "roundrobin"
		}
		if _, ok := config.ExtraParam["fetch.max.bytes"]; !ok {
			config.ExtraParam["fetch.max.bytes"] = 104857600
		}
	}
	c.Consumers[config.Name] = &Consumer{
		Config: config,
		quit:   quit,
	}
	threadCount := config.getNumberOfThreads()
	logger.GetLoggerWithContext(ctx).Info("Starting consumer with threads", zap.String("consumerName", config.Name), zap.Int("threads", threadCount))
	for i := 0; i < threadCount; i++ {
		err := c.createAndStartConsumer(ctx, config, actualProcessor, quit)
		if err != nil {
			close(quit)
			return err
		}
	}
	return nil
}

func (p Producer) Close(ctx context.Context) {
	p.Producer.Flush(5000)
	p.Producer.Close()
	// deliveryErrChan is deliberately left open rather than closed here: the events-draining
	// goroutine drains producer.Events() asynchronously in the background after Close() returns
	// (librdkafka closes that channel once fully flushed), and it may still be mid-send to
	// deliveryErrChan via dispatchDeliveryErr when this method returns. Closing the channel here
	// would race with that send and could panic. Producers are long-lived (one per registered
	// name; NewProducer rejects re-registration), so Close is called once at service shutdown and
	// the dispatcher goroutine exiting with the process is an acceptable trade-off for correctness.
	logger.GetLoggerWithContext(ctx).Info("Closed producer", zap.String("producerName", p.Config.Name))
}

func (c Cluster) CloseConsumer(ctx context.Context, name string) {
	if consumer, ok := c.Consumers[name]; ok {
		close(consumer.quit)
		delete(c.Consumers, name)
		logger.GetLoggerWithContext(ctx).Info("Closed consumer", zap.String("consumerName", consumer.Config.Name))
	}
}

func (c Cluster) createAndStartConsumer(ctx context.Context, config ConsumerConfig, actualProcessor func(message *kafka.Message), quit chan struct{}) error {
	consumer, err := c.createConsumer(config)
	if err != nil {
		return err
	}
	err = consumer.SubscribeTopics(config.Topics, config.PartitionBalanceCallback)
	if err != nil {
		return err
	}
	go startConsuming(ctx, config, consumer, actualProcessor, quit)
	return nil
}

func (c Cluster) createConsumer(config ConsumerConfig) (*kafka.Consumer, error) {

	offSetReset := config.OffsetReset
	if offSetReset.value == "" {
		offSetReset.value = EarliestOffset.value
	}

	configMap := kafka.ConfigMap{
		"bootstrap.servers":        c.Brokers,
		"group.id":                 config.GroupId,
		"auto.offset.reset":        offSetReset.LibraryValue(),
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
	}
	for key, value := range config.ExtraParam {
		configMap.SetKey(key, value)
	}
	CheckAndUpdateSSLConfig(&configMap)
	consumer, err := kafka.NewConsumer(&configMap)
	return consumer, err
}

func adaptToDBHeaders(headers []kafka.Header) []Header {
	kafkaHeaders := make([]Header, len(headers))
	for i, header := range headers {
		key := header.Key
		values := header.Value
		kafkaHeaders[i] = Header{
			Key:   key,
			Value: values,
		}
	}
	return kafkaHeaders
}

func adaptDBHeaders(headers []Header) []kafka.Header {
	kafkaHeaders := make([]kafka.Header, len(headers))
	for i, header := range headers {
		key := header.Key
		values := header.Value
		kafkaHeaders[i] = kafka.Header{
			Key:   key,
			Value: values,
		}
	}
	return kafkaHeaders
}

func adaptMessage(message Message, topic string) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            message.Key,
		Value:          message.Message,
		Headers:        adaptDBHeaders(message.Headers),
	}
}

func startConsuming(ctx context.Context, config ConsumerConfig, consumer *kafka.Consumer, actualProcessor func(message *kafka.Message), quit <-chan struct{}) {
	for {
		select {
		case <-quit:
			err := consumer.Close()
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error while closing Kafka Consumer", zap.Error(err), zap.String("consumerName", config.Name))
			}
			logger.GetLogger().Info("Stopping consumer", zap.String("consumerName", config.Name))
			return
		default:
			if consumer.IsClosed() {
				return
			}
			message, err := consumer.ReadMessage(time.Second * 2)
			if err != nil {
				if !err.(kafka.Error).IsTimeout() {
					logger.GetLoggerWithContext(ctx).Error("Error while consuming message", zap.Error(err), zap.String("consumerName", config.Name))
				}
			} else {
				actualProcessor(message)
				_, err := consumer.StoreMessage(message)
				if err != nil {
					logger.GetLogger().Error("failed to store offset in store", zap.Error(err), zap.String("consumerName", config.Name))
				}
			}
		}
	}
}

func CheckAndUpdateSSLConfig(configMap *kafka.ConfigMap) {
	sslEnabled := utils.GetEnvOrDefault(KafkaSSLEnabled, "false")
	if sslEnabled == "true" {
		configMap.SetKey("security.protocol", "SSL")
		configMap.SetKey("ssl.key.location", utils.GetEnvOrDefault(KafkaSSLKeyLocation, ""))
		configMap.SetKey("ssl.certificate.location", utils.GetEnvOrDefault(KafkaSSLCertLocation, ""))
		configMap.SetKey("ssl.key.password", utils.GetEnvOrDefault(KafkaSSLPassword, ""))
		configMap.SetKey("ssl.ca.location", utils.GetEnvOrDefault(KafkaSSLCA, ""))
	}
}
