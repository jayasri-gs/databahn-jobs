package changeflag

import (
	"context"
	"errors"
	"github.com/databahn-ai/common-utils/svc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/databahn-ai/db-models/alerts_async"

	kafkaconfl "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/databahn-ai/common-utils/ack"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type ResiliencyConfig struct {
	onDown            func(err error)
	connWaitDuration  time.Duration
	connCheckDuration time.Duration
	downCallWait      time.Duration
}

type ResiliencyConfigOption func(*ResiliencyConfig)

func WithConnectionWaitDuration(connWaitDuration time.Duration) ResiliencyConfigOption {
	return func(rc *ResiliencyConfig) {
		rc.connWaitDuration = connWaitDuration
	}
}

func WithConnectionCheckDuration(connCheckDuration time.Duration) ResiliencyConfigOption {
	return func(rc *ResiliencyConfig) {
		rc.connCheckDuration = connCheckDuration
	}
}

func WithDownCallWaitDuration(downCallWait time.Duration) ResiliencyConfigOption {
	return func(rc *ResiliencyConfig) {
		rc.downCallWait = downCallWait
	}
}

func NewResiliencyConfig(onDownCallBack func(err error), options ...ResiliencyConfigOption) *ResiliencyConfig {
	config := &ResiliencyConfig{
		onDown: onDownCallBack,
	}
	for _, opt := range options {
		opt(config)
	}

	if config.connWaitDuration == 0 {
		config.connWaitDuration = 20 * time.Second
	}
	if config.connCheckDuration == 0 {
		config.connCheckDuration = 10 * time.Second
	}

	if config.downCallWait == 0 {
		config.downCallWait = 1 * time.Minute
	}

	return config
}

func NewResiliencyConfigWithShutdownService(shutdownService *svc.ShutdownService, options ...ResiliencyConfigOption) *ResiliencyConfig {
	return NewResiliencyConfig(shutdownService.StopDueToError, options...)
}

type Trigger struct {
	changeTypes           map[string]struct{}
	firstLoadCallBack     func(map[string][]ChangeFlag) []Acknowledgement
	newTriggerCallBack    func(ChangeFlag) *Acknowledgement
	quit                  chan struct{}
	initialLoadDone       bool
	offsetResetDone       map[string]bool
	topicPartitionEofDone map[string]bool
	initialLoad           *sync.Cond
	ackProducer           *ack.AckProducer
	kafkaBootstrapServers string
	redisUrl              string
	consumerGroupId       string
	alertsManager         *alerts_async.AlertsManager
	dataPlaneId           string
	resiliencyConfig      *ResiliencyConfig
}

func NewTriggerWithoutConfigReader(ctx context.Context, kafkaBootstrap, cacheUrl string, changeTypes []string,
	firstLoadCallBack func(map[string][]ChangeFlag) []Acknowledgement,
	newTriggerCallBack func(ChangeFlag) *Acknowledgement, options ...TriggerOption) (*Trigger, error) {
	changeTypesMap := make(map[string]struct{})
	for _, changeType := range changeTypes {
		changeTypesMap[changeType] = struct{}{}
	}
	q := make(chan struct{}, 1)
	cnd := sync.NewCond(&sync.Mutex{})
	ord := make(map[string]bool)
	eof := make(map[string]bool)
	ackProducer, err := ack.NewAckProducer(ctx, kafkaBootstrap)
	if err != nil {
		return nil, err
	}
	t := Trigger{
		quit:                  q,
		changeTypes:           changeTypesMap,
		firstLoadCallBack:     firstLoadCallBack,
		newTriggerCallBack:    newTriggerCallBack,
		initialLoad:           cnd,
		offsetResetDone:       ord,
		topicPartitionEofDone: eof,
		ackProducer:           ackProducer,
		redisUrl:              cacheUrl,
		kafkaBootstrapServers: kafkaBootstrap,
	}
	for _, opt := range options {
		opt(&t)
	}
	err = t.startConsuming(ctx)
	return &t, err
}

type TriggerOption func(*Trigger)

func WithConsumerGroupId(groupId string) TriggerOption {
	return func(t *Trigger) {
		t.consumerGroupId = groupId
	}
}

/*
WithAlertsManager sets the alerts manager and data plane ID for the trigger.
This allows the trigger to send alerts for any acknowledgements that are not successful.
It is optional, and if not set, the trigger will not send alerts.
*/
func WithAlertsManager(alertsManager *alerts_async.AlertsManager, dataPlaneId string) TriggerOption {
	return func(t *Trigger) {
		t.alertsManager = alertsManager
		t.dataPlaneId = dataPlaneId
	}
}

func WithResiliencyConfig(resiliencyConfig *ResiliencyConfig) TriggerOption {
	return func(t *Trigger) {
		if resiliencyConfig != nil {
			t.resiliencyConfig = resiliencyConfig
		}
	}
}

func NewTrigger(ctx context.Context, confReader configuration.ConfigReader, changeTypes []string,
	firstLoadCallBack func(map[string][]ChangeFlag) []Acknowledgement,
	newTriggerCallBack func(ChangeFlag) *Acknowledgement, options ...TriggerOption) (*Trigger, error) {
	changeTypesMap := make(map[string]struct{})
	for _, changeType := range changeTypes {
		changeTypesMap[changeType] = struct{}{}
	}
	q := make(chan struct{}, 1)
	cnd := sync.NewCond(&sync.Mutex{})
	ord := make(map[string]bool)
	eof := make(map[string]bool)
	ackProducer, err := ack.NewAckProducer(ctx, confReader.GetString(configuration.InputKafkaClusterBootstrapServers))
	if err != nil {
		return nil, err
	}
	t := Trigger{
		quit:                  q,
		changeTypes:           changeTypesMap,
		firstLoadCallBack:     firstLoadCallBack,
		newTriggerCallBack:    newTriggerCallBack,
		initialLoad:           cnd,
		offsetResetDone:       ord,
		topicPartitionEofDone: eof,
		ackProducer:           ackProducer,
		redisUrl:              confReader.GetString(configuration.RedisUrl),
		kafkaBootstrapServers: confReader.GetString(configuration.InputKafkaClusterBootstrapServers),
	}
	for _, opt := range options {
		opt(&t)
	}
	err = t.startConsuming(ctx)
	return &t, err
}

func (t *Trigger) Close(ctx context.Context) {
	// Non-blocking send to quit channel in case readMessages goroutine was never started
	select {
	case t.quit <- struct{}{}:
	default:
		// Channel already has a value or no receiver, safe to proceed
	}
	if t.ackProducer != nil {
		t.ackProducer.Close(ctx)
	}
	if t.alertsManager != nil {
		t.alertsManager.Close(ctx)
	}
}

func (t *Trigger) WaitForInitialLoad() {
	t.initialLoad.L.Lock()
	defer t.initialLoad.L.Unlock()
	if !t.initialLoadDone {
		t.initialLoad.Wait()
	}
}

func (t *Trigger) startConsuming(ctx context.Context) error {
	c, err := t.createConsumer(ctx)
	if err != nil {
		return err
	}
	go t.readMessages(ctx, c)
	return nil
}

func (t *Trigger) createConsumer(ctx context.Context) (*kafkaconfl.Consumer, error) {
	kfkBrokers := t.kafkaBootstrapServers
	if t.consumerGroupId == "" {
		groupId, err := os.Hostname()
		if err != nil {
			groupId = "change_flag_" + strings.ReplaceAll(uuid.New().String(), "-", "")
			logger.GetLogger().Error("unable to get hostname, using uuid generated name as consumer group id",
				zap.String("hostname", groupId), zap.Error(err))
		} else {
			logger.GetLogger().Debug("using hostname as consumer group id", zap.String("hostname", groupId))
		}
		t.consumerGroupId = groupId
	}

	configMap := kafkaconfl.ConfigMap{
		"bootstrap.servers":               kfkBrokers,
		"group.id":                        t.consumerGroupId,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"partition.assignment.strategy":   "roundrobin",
		"fetch.wait.max.ms":               1000,
		"session.timeout.ms":              60000,
	}
	consumer, err := kafkaconfl.NewConsumer(&configMap)
	if err != nil {
		return nil, err
	}

	changeFlagTopics := map[string]bool{
		constants.ChangeFlagTopic:           true,
		constants.ChangeFlagPlaygroundTopic: false,
	}
	var validTopics []string

	// Get metadata for all topics
	for topic, mandatory := range changeFlagTopics {
		topicDetails, err := consumer.GetMetadata(&topic, false, 5000)
		if err != nil {
			logger.GetLogger().Error("could not get metadata for topic, skipping", zap.String("topic", topic), zap.Error(err))
			if mandatory {
				consumer.Close()
				return nil, errors.New("could not get metadata for mandatory change flag topic: " + topic)
			}
			continue
		}
		if topicDetails == nil {
			logger.GetLogger().Error("metadata not found for topic, skipping", zap.String("topic", topic))
			if mandatory {
				consumer.Close()
				return nil, errors.New("metadata is nil for mandatory change flag topic: " + topic)
			}
			continue
		}
		if topicDetails.Topics[topic].Error.Code() != kafkaconfl.ErrNoError {
			logger.GetLogger().Error("failed to get kafka metadata", zap.String("topic", topic),
				zap.String("error", topicDetails.Topics[topic].Error.String()))
			if mandatory {
				consumer.Close()
				return nil, errors.New("failed to get kafka metadata for topic: " + topic +
					" error: " + topicDetails.Topics[topic].Error.String())
			}
			continue
		}
		validTopics = append(validTopics, topic)
		for _, topicDetail := range topicDetails.Topics {
			topicName := topicDetail.Topic
			for _, partition := range topicDetail.Partitions {
				id := topicPartitionId(&topicName, partition.ID)
				t.topicPartitionEofDone[id] = false
			}
		}
	}

	// Ensure we have at least one valid topic
	if len(validTopics) == 0 {
		consumer.Close()
		return nil, errors.New("no valid topics found - metadata retrieval failed for all topics")
	}

	logger.GetLogger().Info("change flag topic details", zap.Any("topic partitions", t.topicPartitionEofDone))

	err = consumer.SubscribeTopics(validTopics, t.offsetZeroCallback)
	if err != nil {
		consumer.Close()
		return nil, err
	}
	return consumer, nil
}

func (t *Trigger) readMessages(ctx context.Context, consumer *kafkaconfl.Consumer) {
	initialLoadDone := false
	initialMessages := make(map[string][]ChangeFlag)
	connected := true
	downAt := time.Time{}
	downCalledAt := time.Time{}
	var connectionError error

	connectionCheckTicker := time.NewTicker(24 * time.Hour)
	if t.resiliencyConfig != nil {
		connectionCheckTicker.Stop()
		connectionCheckTicker = time.NewTicker(t.resiliencyConfig.connCheckDuration)
	}

	for {
		select {
		case <-t.quit:
			err := consumer.Close()
			if err != nil {
				logger.GetLogger().Error("failed to close kafka consumer for change flag", zap.Error(err))
			}
			connectionCheckTicker.Stop()
			logger.GetLogger().Info("closed kafka consumer thread for change flag")
			return
		case <-connectionCheckTicker.C:
			if t.resiliencyConfig != nil && !connected {
				downDuration := time.Since(downAt)
				durationSinceLastDownCall := time.Since(downCalledAt)
				if downDuration >= t.resiliencyConfig.connWaitDuration && durationSinceLastDownCall >= t.resiliencyConfig.downCallWait {
					t.resiliencyConfig.onDown(connectionError)
					downCalledAt = time.Now()
				}
			}
		default:
			if consumer.IsClosed() {
				connectionCheckTicker.Stop()
				return
			}
			ev := consumer.Poll(2000)
			switch e := ev.(type) {
			case kafkaconfl.PartitionEOF:
				if !connected {
					connected = true
					downAt = time.Time{}
				}
				id := topicPartitionId(e.Topic, e.Partition)
				t.topicPartitionEofDone[id] = true
				if t.allTopicPartitionsEof() {
					if !initialLoadDone {
						acks := t.firstLoadCallBack(initialMessages)
						initialLoadDone = true
						t.initialLoad.Broadcast()
						t.initialLoadDone = true
						logger.GetLogger().Info("change flag eof of all topic partitions", zap.Any("status", t.topicPartitionEofDone))
						for dataType, data := range initialMessages {
							logger.GetLogger().Info("change flag read initially", zap.Any("dataType", dataType), zap.Int("count", len(data)))
						}
						t.SendAcknowledgements(ctx, acks)
						logger.GetLogger().Info("initial acknowledgements sent", zap.Int("count", len(acks)))
					}
				} else {
					logger.GetLogger().Debug("change flag waiting for more topic partitions to eof", zap.Any("status", t.topicPartitionEofDone))
				}
			case *kafkaconfl.Message:
				if !connected {
					connected = true
					downAt = time.Time{}
				}
				flag, err := prepareChangeFlag(e.Value, e.Headers, e.Key, *e.TopicPartition.Topic)
				if err != nil {
					logger.GetLogger().Error("change flag entity header not found",
						zap.Int64("offset", int64(e.TopicPartition.Offset)), zap.Int32("partition", e.TopicPartition.Partition), zap.Error(err))
				} else if _, ok := t.changeTypes[flag.EntityType]; ok {
					if initialLoadDone {
						ack := t.newTriggerCallBack(*flag)
						if ack != nil {
							t.SendAcknowledgements(ctx, []Acknowledgement{*ack})
						} else {
							logger.GetLogger().Debug("acknowledgement is empty")
						}
					} else {
						initialMessages[flag.EntityType] = append(initialMessages[flag.EntityType], *flag)
					}
					logger.GetLogger().Debug("change flag message read", zap.String("entityType", flag.EntityType),
						zap.String("entityId", flag.EntityId), zap.Int64("offset", int64(e.TopicPartition.Offset)),
						zap.Int32("partition", e.TopicPartition.Partition))
				}
			case kafkaconfl.Error:
				logger.GetLogger().Error("failed to read kafka message from change flag", zap.Error(e))
				isConnectionError := e.Code() == kafkaconfl.ErrAllBrokersDown ||
					e.Code() == kafkaconfl.ErrTransport ||
					e.Code() == kafkaconfl.ErrResolve ||
					e.Code() == kafkaconfl.ErrBrokerNotAvailable ||
					e.Code() == kafkaconfl.ErrUnknownPartition ||
					e.Code() == kafkaconfl.ErrNetworkException ||
					e.Code() == kafkaconfl.ErrUnknownBroker ||
					e.Code() == kafkaconfl.ErrUnknownTopic
				if isConnectionError && connected {
					connectionError = errors.New(e.String())
					connected = false
					downAt = time.Now()
					logger.GetLogger().Error("kafka connection lost for change flag consumer")
				}
			}
		}
	}
}

func prepareChangeFlag(value []byte, headers []kafkaconfl.Header, key []byte, topicName string) (*ChangeFlag, error) {
	changeFlag := ChangeFlag{}
	for _, hdr := range headers {
		if hdr.Key == constants.HeaderTenantId {
			changeFlag.TenantId = string(hdr.Value)
		}
		if hdr.Key == constants.HeaderRequestId {
			changeFlag.RequestId = string(hdr.Value)
		}
		if hdr.Key == constants.HeaderEntityType {
			changeFlag.EntityType = string(hdr.Value)
		}
		if hdr.Key == constants.HeaderEntityName {
			changeFlag.EntityName = string(hdr.Value)
		}
		if hdr.Key == constants.HeaderAction {
			changeFlag.Action = string(hdr.Value)
		}
	}
	if changeFlag.TenantId == "" || changeFlag.RequestId == "" || changeFlag.EntityType == "" || changeFlag.Action == "" {
		logger.GetLogger().Error("missing headers in change flag message", zap.Any("flag", changeFlag))
		return nil, errors.New("missing headers in change flag message")
	}
	changeFlag.EntityId = string(key)
	changeFlag.Entity = value
	changeFlag.IsPlayground = topicName == constants.ChangeFlagPlaygroundTopic
	return &changeFlag, nil
}

func (t *Trigger) offsetZeroCallback(consumer *kafkaconfl.Consumer, event kafkaconfl.Event) error {
	switch ev := event.(type) {
	case kafkaconfl.AssignedPartitions:
		if consumer.IsClosed() {
			return nil
		}
		var newOffsets []kafkaconfl.TopicPartition
		for _, tp := range ev.Partitions {
			key := topicPartitionId(tp.Topic, tp.Partition)
			if !t.offsetResetDone[key] {
				tp.Offset = kafkaconfl.OffsetBeginning
				newOffsets = append(newOffsets, tp)
				t.offsetResetDone[key] = true
			} else {
				logger.GetLogger().Warn("rebalancing already offset reset kafka topic partition",
					zap.String("topic", *tp.Topic), zap.Int32("partition", tp.Partition))
			}
		}
		if len(newOffsets) > 0 {
			err := consumer.Assign(newOffsets)
			if err != nil {
				logger.GetLogger().Error("failed to assign partitions to consumer", zap.Error(err))
			} else {
				logger.GetLogger().Info("partition reassignment done", zap.Any("new-offsets", newOffsets))
			}
		}

	}
	return nil
}

func (t *Trigger) allTopicPartitionsEof() bool {
	// If no topic partitions are tracked, we can't consider them all EOF
	if len(t.topicPartitionEofDone) == 0 {
		return false
	}
	for _, tp := range t.topicPartitionEofDone {
		if !tp {
			return false
		}
	}
	return true
}

func topicPartitionId(topic *string, partition int32) string {
	return (*topic) + ":" + strconv.FormatInt(int64(partition), 10)
}
