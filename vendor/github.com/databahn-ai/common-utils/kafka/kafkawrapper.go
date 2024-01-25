package kafka

import (
	"context"
	"errors"
	"runtime"
	"time"

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
}

type Producer struct {
	Producer *kafka.Producer
	Config   ProducerConfig
}

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
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": c.Brokers,
	}

	for key, value := range config.ExtraParam {
		configMap.SetKey(key, value)
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logger.GetLoggerWithContext(ctx).Error("Failed to send to Kafka topic", zap.Error(ev.TopicPartition.Error),
						zap.String("producerName", config.Name), zap.String("Topic", config.Topic))
				}
			case kafka.Error:
				logger.GetLoggerWithContext(ctx).Error("Failed to send to Kafka cluster", zap.Error(ev),
					zap.String("producerName", config.Name), zap.String("Topic", config.Topic))
			}
		}
	}()

	return &Producer{
		Producer: producer,
		Config:   config,
	}, nil
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
	report := make(chan kafka.Event)
	defer close(report)
	kafkaMessage := adaptMessage(message, p.Config.Topic)
	err := p.Producer.Produce(kafkaMessage, report)
	if err != nil {
		return nil
	}
	e := <-report
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
}

func (p Producer) SendAsync(message Message, callback func(err error)) {
	kafkaMessage := adaptMessage(message, p.Config.Topic)
	err := p.Producer.Produce(kafkaMessage, nil)
	if err != nil && callback != nil {
		callback(err)
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
	consumer, err := kafka.NewConsumer(&configMap)
	return consumer, err
}

func adaptToDBHeaders(headers []kafka.Header) []Header {
	kafkaHeaders := make([]Header, len(headers))
	for _, header := range headers {
		key := header.Key
		values := header.Value
		kafkaHeaders = append(kafkaHeaders, Header{
			Key:   key,
			Value: values,
		})
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
