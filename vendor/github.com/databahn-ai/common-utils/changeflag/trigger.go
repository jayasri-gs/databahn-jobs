package changeflag

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"

	kafkaconfl "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/databahn-ai/common-utils/ack"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

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
}

func NewTriggerWithoutConfigReader(ctx context.Context, kafkaBootstrap, cacheUrl string, changeTypes []string,
	firstLoadCallBack func(map[string][]ChangeFlag) []Acknowledgement,
	newTriggerCallBack func(ChangeFlag) *Acknowledgement, options ...TriggerOption) (*Trigger, error) {
	changeTypesMap := make(map[string]struct{})
	for _, changeType := range changeTypes {
		changeTypesMap[changeType] = struct{}{}
	}
	q := make(chan struct{})
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

func NewTrigger(ctx context.Context, confReader configuration.ConfigReader, changeTypes []string,
	firstLoadCallBack func(map[string][]ChangeFlag) []Acknowledgement,
	newTriggerCallBack func(ChangeFlag) *Acknowledgement, options ...TriggerOption) (*Trigger, error) {
	changeTypesMap := make(map[string]struct{})
	for _, changeType := range changeTypes {
		changeTypesMap[changeType] = struct{}{}
	}
	q := make(chan struct{})
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

func (t *Trigger) Close() {
	t.quit <- struct{}{}
	t.ackProducer.Close(context.Background())
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

	changeFlagTopic := constants.ChangeFlagTopic
	topicDetails, err := consumer.GetMetadata(&changeFlagTopic, false, 5000)
	if err != nil {
		return nil, err
	}
	for _, topicDetail := range topicDetails.Topics {
		topic := topicDetail.Topic
		for _, partition := range topicDetail.Partitions {
			id := topicPartitionId(&topic, partition.ID)
			t.topicPartitionEofDone[id] = false
		}
	}
	logger.GetLogger().Info("change flag topic details", zap.Any("topic partitions", t.topicPartitionEofDone))

	err = consumer.Subscribe(changeFlagTopic, t.offsetZeroCallback)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func (t *Trigger) readMessages(ctx context.Context, consumer *kafkaconfl.Consumer) {
	initialLoadDone := false
	initialMessages := make(map[string][]ChangeFlag)
	for {
		select {
		case <-t.quit:
			err := consumer.Close()
			if err != nil {
				logger.GetLogger().Error("failed to close kafka consumer for change flag", zap.Error(err))
			}
			logger.GetLogger().Info("closed kafka consumer thread for change flag")
			return
		default:
			if consumer.IsClosed() {
				return
			}
			ev := consumer.Poll(2000)
			switch e := ev.(type) {
			case kafkaconfl.PartitionEOF:
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
				flag, err := prepareChangeFlag(e.Value, e.Headers, e.Key)
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
				}
			case kafkaconfl.Error:
				logger.GetLogger().Error("failed to read kafka message from change flag", zap.Error(e))
			}
		}
	}
}

func prepareChangeFlag(value []byte, headers []kafkaconfl.Header, key []byte) (*ChangeFlag, error) {
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
				tp.Offset = kafkaconfl.Offset(0)
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
	for _, tp := range t.topicPartitionEofDone {
		if tp == false {
			return false
		}
	}
	return true
}

func topicPartitionId(topic *string, partition int32) string {
	return (*topic) + ":" + strconv.FormatInt(int64(partition), 10)
}
