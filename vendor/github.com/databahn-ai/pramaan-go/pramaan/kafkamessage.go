package pramaan

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/PaesslerAG/jsonpath"
	"github.com/databahn-ai/common-utils/kafka"
)

func GetHeader(msg *kafka.Message, header string) string {
	for _, h := range msg.Headers {
		if h.Key == header {
			return string(h.Value)
		}
	}
	return ""
}

/*
KafkaMessagesVerifier is a utility to verify Kafka messages in tests.
It provides methods to check for the presence of messages with specific bodies, keys, headers, and JSON path values.
*/
type KafkaMessagesVerifier struct {
	t        *testing.T
	messages []kafka.Message
}

func NewKafkaMessagesVerifier(t *testing.T, message []kafka.Message) *KafkaMessagesVerifier {
	return &KafkaMessagesVerifier{t: t, messages: message}
}

/*
ExpectMessageCount checks if the number of messages received matches the expected count.
*/
func (k *KafkaMessagesVerifier) ExpectMessageCount(expectedCount int) {
	if expectedCount != len(k.messages) {
		k.t.Fatalf("Expected %d messages, got %d", expectedCount, len(k.messages))
	}
}

/*
ExpectAMessageWithBody checks if there is a message with the specified body.
If found, it returns the message; otherwise, it fails the test.
*/
func (k *KafkaMessagesVerifier) ExpectAMessageWithBody(expectedMessage string) *kafka.Message {
	for _, message := range k.messages {
		if string(message.Message) == expectedMessage {
			return &message
		}
	}
	k.t.Fatalf("Expected a message with body %s, but not found", expectedMessage)
	return nil
}

/*
ExpectAMessageContainingBody checks if there is a message that contains the specified substring.
*/
func (k *KafkaMessagesVerifier) ExpectAMessageContainingBody(expectedMessage string) *kafka.Message {
	for _, message := range k.messages {
		if strings.Contains(string(message.Message), expectedMessage) {
			return &message
		}
	}
	k.t.Fatalf("Expected a message with body %s, but not found", expectedMessage)
	return nil
}

/*
ExpectAMessageWithKey checks if there is a message with the specified key.
*/
func (k *KafkaMessagesVerifier) ExpectAMessageWithKey(expectedKey string) *kafka.Message {
	for _, message := range k.messages {
		if string(message.Key) == expectedKey {
			return &message
		}
	}
	k.t.Fatalf("Expected a message with key %s, but not found", expectedKey)
	return nil
}

/*
ExpectAMessageWithHeaders checks if there is a message with the specified headers.
All headers must match for the message to be considered a match.
If found, it returns the message; otherwise, it fails the test.
*/
func (k *KafkaMessagesVerifier) ExpectAMessageWithHeaders(expectedHeaders map[string]string) *kafka.Message {
	for _, message := range k.messages {
		actualHeaders := make(map[string]string)
		for _, header := range message.Headers {
			actualHeaders[header.Key] = string(header.Value)
		}
		matchingHeaderCount := 0
		for ek, ev := range expectedHeaders {
			if actualHeaders[ek] != ev {
				break
			} else {
				matchingHeaderCount++
			}
		}
		if matchingHeaderCount == len(expectedHeaders) {
			return &message
		}
	}
	k.t.Fatalf("Expected a message with headers %v, but not found", expectedHeaders)
	return nil
}

/*
ExpectExactOneMessageWithHeaders checks if there is exactly one message with the specified headers.
If exactly one message is found, it returns that message; otherwise, it fails the test.
*/
func (k *KafkaMessagesVerifier) ExpectExactOneMessageWithHeaders(expectedHeaders map[string]string) *kafka.Message {
	messageCounter := 0
	var msg *kafka.Message
	for _, message := range k.messages {
		actualHeaders := make(map[string]string)
		for _, header := range message.Headers {
			actualHeaders[header.Key] = string(header.Value)
		}
		matchingHeaderCount := 0
		for ek, ev := range expectedHeaders {
			if actualHeaders[ek] != ev {
				break
			} else {
				matchingHeaderCount++
			}
		}
		if matchingHeaderCount == len(expectedHeaders) {
			messageCounter++
			msg = &message
		}
	}
	if messageCounter == 1 {
		return msg
	} else if messageCounter > 1 {
		k.t.Fatalf("Expected exactly one message with headers %v, but found %d", expectedHeaders, messageCounter)
	}
	k.t.Fatalf("Expected a message with headers %v, but not found", expectedHeaders)
	return nil
}

/*
ExpectAMessageWithBodyJsonPath checks if there is a message with a body that matches the specified JSON path query and value.
If found, it returns the message; otherwise, it fails the test.
If the message body cannot be unmarshalled or the JSON path query fails, it will also fail the test.
*/
func (k *KafkaMessagesVerifier) ExpectAMessageWithBodyJsonPath(jsonPathQuery string, value any) *kafka.Message {
	for _, message := range k.messages {
		v := interface{}(nil)
		err := json.Unmarshal(message.Message, &v)
		if err != nil {
			k.t.Fatalf("Failed to unmarshal message body %s, err:%v", string(message.Message), err)
		}
		valueMatch, err := jsonpath.Get(jsonPathQuery, v)
		if err != nil {
			k.t.Fatalf("Failed to get json path %s in %s, err: %v", string(message.Message), jsonPathQuery, err)
		}
		if valueMatch == nil {
			k.t.Fatalf("No jsonpath match %s in %s, err: %v", jsonPathQuery, string(message.Message), err)
		}
		if reflect.DeepEqual(valueMatch, value) {
			return &message
		}
	}
	k.t.Fatalf("Expected a message with body jsonpath(%s)=%v, but not found", jsonPathQuery, value)
	return nil
}
