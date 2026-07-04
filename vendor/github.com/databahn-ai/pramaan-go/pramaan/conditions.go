package pramaan

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/PaesslerAG/jsonpath"
)

/*
Condition are used to verify if a certain condition is met for a given parameter.
String representation of the condition is also provided for logging or debugging purposes.
The `Condition` interface defines a method `Verify` that takes a parameter of type `T` and returns a boolean indicating whether the condition is satisfied.
*/
type Condition[T any] interface {
	Verify(param T) bool
	String() string
}

/*
KafkaMessageWithHeaderCondition is a condition that checks if there are any messages in a specific Kafka topic with a specific header key and value.
*/
type KafkaMessageWithHeaderCondition struct {
	topic       string
	headerKey   string
	headerValue string
}

func NewKafkaMessageWithHeaderCondition(topic string, headerKey string, headerValue string) KafkaMessageWithHeaderCondition {
	return KafkaMessageWithHeaderCondition{topic: topic, headerKey: headerKey, headerValue: headerValue}
}

func (k KafkaMessageWithHeaderCondition) Verify(param *Collector) bool {
	if messages := param.GetMessages(k.topic); messages != nil {
		for _, message := range messages {
			if GetHeader(&message, k.headerKey) == k.headerValue {
				return true
			}
		}
	}
	return false
}

func (k KafkaMessageWithHeaderCondition) String() string {
	return "KafkaMessageWithHeaderCondition: topic=" + k.topic + ", headerKey=" + k.headerKey + ", headerValue=" + k.headerValue
}

/*
KafkaMessageWithBodyJsonPathCondition is a condition that checks if there are any messages in a specific Kafka topic with a body that matches a specific JSON path and expected value.
*/
type KafkaMessageWithBodyJsonPathCondition struct {
	topic         string
	jsonPath      string
	expectedValue string
}

func NewKafkaMessageWithBodyJsonPathCondition(topic string, jsonPath string, expectedValue string) KafkaMessageWithBodyJsonPathCondition {
	return KafkaMessageWithBodyJsonPathCondition{topic: topic, jsonPath: jsonPath, expectedValue: expectedValue}
}

func (k KafkaMessageWithBodyJsonPathCondition) Verify(param *Collector) bool {
	for _, message := range param.GetMessages(k.topic) {
		v := interface{}(nil)
		err := json.Unmarshal(message.Message, &v)
		if err != nil {
			println("Failed to unmarshal message body", message.Message, err)
		}
		valueMatch, err := jsonpath.Get(k.jsonPath, v)
		if err != nil {
			println("Failed to get jsonpath", k.jsonPath, v, err)
		}
		if reflect.DeepEqual(valueMatch, k.expectedValue) {
			return true
		}
	}
	return false
}

func (k KafkaMessageWithBodyJsonPathCondition) String() string {
	return "KafkaMessageWithBodyJsonPathCondition: topic=" + k.topic + ", jsonPath=" + k.jsonPath + ", expectedValue=" + k.expectedValue
}

/*
MultipleKafkaMessagesWithHeaderCondition is a condition that checks if there are a specific number of messages in a specific Kafka topic with a specific header key and value.
*/
type MultipleKafkaMessagesWithHeaderCondition struct {
	topic         string
	headerKey     string
	headerValue   string
	expectedCount int
}

func NewMultipleKafkaMessagesWithHeaderCondition(topic, headerKey, headerValue string, count int) MultipleKafkaMessagesWithHeaderCondition {

	return MultipleKafkaMessagesWithHeaderCondition{
		topic:         topic,
		headerKey:     headerKey,
		headerValue:   headerValue,
		expectedCount: count,
	}
}

func (k MultipleKafkaMessagesWithHeaderCondition) String() string {
	return fmt.Sprintf("MultipleKafkaMessagesWithHeaderCondition: topic=%s, headerKey=%s, headerValue=%s, count=%d", k.topic, k.headerKey, k.headerValue, k.expectedCount)
}

func (k MultipleKafkaMessagesWithHeaderCondition) Verify(param *Collector) bool {
	matchingCount := 0
	if messages := param.GetMessages(k.topic); messages != nil {
		for _, message := range messages {
			if GetHeader(&message, k.headerKey) == k.headerValue {
				matchingCount++
			}
		}
	}
	return matchingCount == k.expectedCount
}

/*
OpenSearchDocumentExistsCondition checks that an OpenSearch document was fetched successfully.
*/
type OpenSearchDocumentExistsCondition struct{}

func (OpenSearchDocumentExistsCondition) Verify(document *OpenSearchDocument) bool {
	return document != nil && document.Raw != nil
}

func (OpenSearchDocumentExistsCondition) String() string {
	return "OpenSearchDocumentExistsCondition"
}

/*
OpenSearchSourceCondition checks a condition against the document _source.
*/
type OpenSearchSourceCondition struct {
	description string
	verify      func(map[string]interface{}) bool
}

func NewOpenSearchSourceCondition(description string, verify func(map[string]interface{}) bool) OpenSearchSourceCondition {
	return OpenSearchSourceCondition{
		description: description,
		verify:      verify,
	}
}

func (c OpenSearchSourceCondition) Verify(document *OpenSearchDocument) bool {
	source, ok := document.Source()
	if !ok {
		return false
	}
	return c.verify(source)
}

func (c OpenSearchSourceCondition) String() string {
	return c.description
}
