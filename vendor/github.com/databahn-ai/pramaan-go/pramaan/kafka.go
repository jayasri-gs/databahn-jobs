package pramaan

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	kafkaconfl "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	dbkafka "github.com/databahn-ai/common-utils/kafka"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/network"
)

/*
KafkaPramaan is a utility to start Kafka for module testing.
It can start Kafka with predefined topics to create and start consuming messages for given topics for verification.
You can send messages to kafka topics with it.
*/
type KafkaPramaan struct {
	t              TestLogger
	Container      *kafka.KafkaContainer
	ExternalBroker string
	NetworkBroker  string
	cluster        *dbkafka.Cluster
	collector      *Collector
	producers      map[string]*dbkafka.Producer
}

type TopicDetails struct {
	Topic      string
	Partitions int
}

func NewKafkaPramaan(ctx context.Context, t TestLogger, dockerNetwork *testcontainers.DockerNetwork) *KafkaPramaan {
	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
		network.WithNetwork([]string{"pramaankafka"}, dockerNetwork),
	)
	if err != nil {
		t.Fatalf("Could not create kafka container %v", err)
	}
	host, err := kafkaContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get host: %v", err)
	}

	port, err := kafkaContainer.MappedPort(ctx, "9093")
	if err != nil {
		log.Fatalf("failed to get port: %v", err)
	}
	externalBrokers := fmt.Sprintf("%s:%d", host, port.Num())
	networkBrokers := "pramaankafka:9092"
	cluster := dbkafka.NewKafkaCluster(t.Name(), externalBrokers)
	collector := &Collector{
		messages: make(map[string][]dbkafka.Message),
		mx:       sync.RWMutex{},
	}
	return &KafkaPramaan{t: t, Container: kafkaContainer,
		ExternalBroker: externalBrokers,
		NetworkBroker:  networkBrokers,
		cluster:        cluster,
		producers:      map[string]*dbkafka.Producer{},
		collector:      collector}
}

/*
CreateTopics creates the topics in the Kafka cluster.
It takes a slice of TopicDetails which contains the topic name and number of partitions.
*/
func (k *KafkaPramaan) CreateTopics(ctx context.Context, t TestLogger, topics []TopicDetails) {
	adminClient, err := kafkaconfl.NewAdminClient(&kafkaconfl.ConfigMap{
		"bootstrap.servers": k.ExternalBroker,
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}

	var topicSpecifications []kafkaconfl.TopicSpecification
	for _, topic := range topics {
		topicSpecifications = append(topicSpecifications, kafkaconfl.TopicSpecification{
			Topic:             topic.Topic,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: 1,
		})
	}

	_, err = adminClient.CreateTopics(ctx, topicSpecifications)
	if err != nil {
		t.Fatalf("Failed to create topics: %v", err)
	}
}

type Consumer struct {
	Collector *Collector
}

func (c Consumer) Process(message *dbkafka.ConsumedMessage[string, string]) error {
	key := []byte(message.Key)
	value := []byte(*message.Value)
	topic := message.Topic
	c.Collector.Collect(topic, dbkafka.Message{Key: key, Message: value, Headers: message.Headers})
	return nil
}

func (c Consumer) KeyDeserialize(b []byte) (string, error) {
	return string(b), nil
}
func (c Consumer) ValueDeserialize(b []byte) (string, error) {
	return string(b), nil
}

type Collector struct {
	messages map[string][]dbkafka.Message
	mx       sync.RWMutex
}

/*
Collect collects messages for a given topic.
It stores the messages in a map where the key is the topic name and the value is a slice of messages.
*/
func (c *Collector) Collect(topic string, message dbkafka.Message) {
	c.mx.Lock()
	defer c.mx.Unlock()
	if c.messages[topic] == nil {
		c.messages[topic] = make([]dbkafka.Message, 0)
	}
	c.messages[topic] = append(c.messages[topic], message)

}

/*
GetMessages retrieves the messages for a given topic.
It returns a slice of messages for the specified topic.
*/
func (c *Collector) GetMessages(topic string) []dbkafka.Message {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.messages[topic]
}

/*
StartConsumers starts consumers for the specified topics to listen.
*/
func (k *KafkaPramaan) StartConsumers(ctx context.Context, t TestLogger, topics []string) {
	c := &Consumer{
		Collector: k.collector,
	}
	consumerConfig := dbkafka.ConsumerConfig{
		Name:        t.Name(),
		GroupId:     t.Name(),
		NoOfThreads: 1,
		Topics:      topics,
	}
	var consumer dbkafka.DetailedKafkaConsumer[string, string] = c
	err := dbkafka.NewDetailedConsumer(*k.cluster, ctx, consumerConfig, consumer)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
}

/*
GetMessages retrieves messages for a given topic from the collector.
It returns a slice of messages for the specified topic.
*/
func (k *KafkaPramaan) GetMessages(topic string) []dbkafka.Message {
	return k.collector.GetMessages(topic)
}

func (k *KafkaPramaan) SendMessagesSync(ctx context.Context, t TestLogger, topic string, messages []dbkafka.Message) {
	if k.producers[topic] == nil {
		producer, err := k.cluster.NewProducer(ctx, dbkafka.ProducerConfig{
			Name:  topic,
			Topic: topic,
		})
		if err != nil {
			t.Fatalf("Failed to create producer: %v", err)
		}
		k.producers[topic] = producer
	}
	for _, message := range messages {
		err := k.producers[topic].SendSync(ctx, message)
		if err != nil {
			t.Fatalf("Failed to send message: %v", err)
		}
	}
}

/*
WaitForCondition waits for a specific condition to be met on the collector.
It checks the condition at regular intervals until the timeout is reached.
If the condition is met, it returns; otherwise, it fails the test after the timeout.
It takes a Condition[*Collector] which is a function that checks the condition on the collector.
*/
func (k *KafkaPramaan) WaitForCondition(condition Condition[*Collector], checkInterval time.Duration, timeout time.Duration) {
	if timeout < checkInterval {
		k.t.Fatalf("Timeout should be greater than check interval")
	}
	timeOutTicker := time.NewTicker(timeout)
	checkTicker := time.NewTicker(checkInterval)
	defer timeOutTicker.Stop()
	defer checkTicker.Stop()
	for {
		select {
		case <-timeOutTicker.C:
			k.t.Fatalf("Timeout waiting for condition: %s", condition.String())
		case <-checkTicker.C:
			check := condition.Verify(k.collector)
			if check {
				return
			}
		}
	}
}
