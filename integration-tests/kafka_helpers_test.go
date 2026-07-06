//go:build integration

package integrationtests

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PaesslerAG/jsonpath"
	dbkafka "github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/pramaan-go/pramaan"
)

const lastReminderAlertsSection = "Last Reminder Alerts"

func KafkaBodyJSONPathEquals(message dbkafka.Message, jsonPath string, expected any) bool {
	var body any
	if err := json.Unmarshal(message.Message, &body); err != nil {
		return false
	}
	value, err := jsonpath.Get(jsonPath, body)
	if err != nil || value == nil {
		return false
	}
	return reflect.DeepEqual(value, expected)
}

func WaitForKafkaMessage(
	t *testing.T,
	kafka *pramaan.KafkaPramaan,
	topic string,
	match func(dbkafka.Message) bool,
	pollRate, timeout time.Duration,
) dbkafka.Message {
	t.Helper()

	if timeout < pollRate {
		t.Fatal("timeout must be >= poll rate")
	}

	deadline := time.Now().Add(timeout)
	for {
		for _, message := range kafka.GetMessages(topic) {
			if match(message) {
				return message
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for kafka message on topic %s", topic)
		}
		time.Sleep(pollRate)
	}
}

func AssertKafkaMessageJSONPath(t *testing.T, message dbkafka.Message, jsonPath string, want any) {
	t.Helper()

	got, ok := kafkaBodyJSONPathValue(message, jsonPath)
	if !ok {
		t.Fatalf("jsonpath %s not found in message body", jsonPath)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("jsonpath %s = %v, want %v", jsonPath, got, want)
	}
}

func AssertKafkaMessageHeader(t *testing.T, message dbkafka.Message, key, want string) {
	t.Helper()

	if got := pramaan.GetHeader(&message, key); got != want {
		t.Fatalf("header %s = %q, want %q", key, got, want)
	}
}

func AssertKafkaMessageCount(t *testing.T, kafka *pramaan.KafkaPramaan, topic string, want int, match func(dbkafka.Message) bool) {
	t.Helper()

	got := countKafkaMessagesMatching(t, kafka, topic, match)
	if got != want {
		t.Fatalf("topic %s: want %d matching messages, got %d", topic, want, got)
	}
}

func countKafkaMessagesMatching(t *testing.T, kafka *pramaan.KafkaPramaan, topic string, match func(dbkafka.Message) bool) int {
	t.Helper()

	count := 0
	for _, message := range kafka.GetMessages(topic) {
		if match(message) {
			count++
		}
	}
	return count
}

func kafkaBodyJSONPathValue(message dbkafka.Message, jsonPath string) (any, bool) {
	var body any
	if err := json.Unmarshal(message.Message, &body); err != nil {
		return nil, false
	}
	value, err := jsonpath.Get(jsonPath, body)
	if err != nil || value == nil {
		return nil, false
	}
	return value, true
}

func AssertNotificationSentUpdate(
	t *testing.T,
	kafkaPramaan *pramaan.KafkaPramaan,
	alertID string,
	wantCount int,
	notBeforeMillis int64,
) {
	t.Helper()

	matches := 0
	for _, message := range kafkaPramaan.GetMessages(AlertIndexingTopic) {
		if pramaan.GetHeader(&message, "action") != "notification_sent" {
			continue
		}
		if !KafkaBodyJSONPathEquals(message, "$.id", alertID) {
			continue
		}
		matches++
		assertNotificationSentMessage(t, message, alertID, wantCount, notBeforeMillis)
	}
	if matches != 1 {
		t.Fatalf("alert %s: want exactly 1 notification_sent message, got %d", alertID, matches)
	}
}

func assertNotificationSentMessage(
	t *testing.T,
	message dbkafka.Message,
	alertID string,
	wantCount int,
	notBeforeMillis int64,
) {
	t.Helper()

	AssertKafkaMessageHeader(t, message, "action", "notification_sent")
	AssertKafkaMessageJSONPath(t, message, "$.id", alertID)

	count, ok := kafkaBodyJSONPathValue(message, "$.notificationCount")
	if !ok {
		t.Fatalf("notification_sent for alert %s missing notificationCount", alertID)
	}
	countFloat, ok := count.(float64)
	if !ok || int(countFloat) != wantCount {
		t.Fatalf("alert %s notificationCount = %v, want %d", alertID, count, wantCount)
	}

	lastNotificationTime, ok := kafkaBodyJSONPathValue(message, "$.lastNotificationTime")
	if !ok {
		t.Fatalf("notification_sent for alert %s missing lastNotificationTime", alertID)
	}
	lastNotificationMillis, ok := lastNotificationTime.(float64)
	if !ok || int64(lastNotificationMillis) < notBeforeMillis {
		t.Fatalf("alert %s lastNotificationTime = %v, want >= %d", alertID, lastNotificationTime, notBeforeMillis)
	}
}

func AssertNotificationSentBackfillsActivationTime(
	t *testing.T,
	message dbkafka.Message,
	alertID string,
	wantCount int,
	wantLastActivationTime int64,
	notBeforeMillis int64,
) {
	t.Helper()

	assertNotificationSentMessage(t, message, alertID, wantCount, notBeforeMillis)

	lastActivationTime, ok := kafkaBodyJSONPathValue(message, "$.lastActivationTime")
	if !ok {
		t.Fatalf("notification_sent for alert %s missing lastActivationTime", alertID)
	}
	lastActivationMillis, ok := lastActivationTime.(float64)
	if !ok || int64(lastActivationMillis) != wantLastActivationTime {
		t.Fatalf("alert %s lastActivationTime = %v, want %d", alertID, lastActivationTime, wantLastActivationTime)
	}
}

func AssertNoNotificationSent(t *testing.T, kafkaPramaan *pramaan.KafkaPramaan, alertID string) {
	t.Helper()
	AssertKafkaMessageCount(t, kafkaPramaan, AlertIndexingTopic, 0, func(message dbkafka.Message) bool {
		return pramaan.GetHeader(&message, "action") == "notification_sent" &&
			KafkaBodyJSONPathEquals(message, "$.id", alertID)
	})
}

func kafkaEmailBody(t *testing.T, message dbkafka.Message) string {
	t.Helper()

	value, ok := kafkaBodyJSONPathValue(message, "$.body")
	if !ok {
		t.Fatal("email kafka message missing $.body")
	}
	body, ok := value.(string)
	if !ok || body == "" {
		t.Fatalf("email kafka message $.body = %T, want non-empty string", value)
	}
	return body
}

func AssertKafkaEmailBodySections(
	t *testing.T,
	message dbkafka.Message,
	newEntity string,
	reminderEntity string,
) {
	t.Helper()

	body := kafkaEmailBody(t, message)
	newStart, newEnd := emailHTMLSectionRange(body, "New Alerts", "Reminder Alerts")
	reminderStart, reminderEnd := emailHTMLSectionRange(body, "Reminder Alerts", "")

	assertEntityInHTMLRange(t, body, newStart, newEnd, newEntity, "New Alerts")
	assertEntityNotInHTMLRange(t, body, newStart, newEnd, reminderEntity, "New Alerts")
	assertEntityInHTMLRange(t, body, reminderStart, reminderEnd, reminderEntity, "Reminder Alerts")
	assertEntityNotInHTMLRange(t, body, reminderStart, reminderEnd, newEntity, "Reminder Alerts")
}

func AssertKafkaEmailBodyEntityAbsent(t *testing.T, message dbkafka.Message, entityName string) {
	t.Helper()

	body := kafkaEmailBody(t, message)
	if strings.Contains(body, entityName) {
		t.Fatalf("entity %q should not appear in email body", entityName)
	}
}

func AssertKafkaEmailBodyEntityInSection(
	t *testing.T,
	message dbkafka.Message,
	sectionHeading, nextSectionHeading, entityName string,
) {
	t.Helper()

	body := kafkaEmailBody(t, message)
	start, end := emailHTMLSectionRange(body, sectionHeading, nextSectionHeading)
	assertEntityInHTMLRange(t, body, start, end, entityName, sectionHeading)
}

func AssertKafkaEmailBodyEntityAbsentFromSection(
	t *testing.T,
	message dbkafka.Message,
	sectionHeading, nextSectionHeading, entityName string,
) {
	t.Helper()

	body := kafkaEmailBody(t, message)
	start, end := emailHTMLSectionRange(body, sectionHeading, nextSectionHeading)
	if start < 0 {
		return
	}
	assertEntityNotInHTMLRange(t, body, start, end, entityName, sectionHeading)
}

func emailHTMLSectionRange(body, heading, nextHeading string) (start, end int) {
	start = findSectionHeadingStart(body, heading)
	if start < 0 {
		return -1, -1
	}
	end = len(body)
	if nextHeading == "" {
		return start, end
	}
	remainder := body[start+len(heading):]
	next := findSectionHeadingStart(remainder, nextHeading)
	if next >= 0 {
		end = start + len(heading) + next
	}
	return start, end
}

func findSectionHeadingStart(body, heading string) int {
	searchFrom := 0
	for {
		idx := strings.Index(body[searchFrom:], heading)
		if idx < 0 {
			return -1
		}
		absIdx := searchFrom + idx
		if absIdx > 0 && body[absIdx-1] != '>' {
			searchFrom = absIdx + len(heading)
			continue
		}
		return absIdx
	}
}

func assertEntityInHTMLRange(t *testing.T, body string, start, end int, entity, section string) {
	t.Helper()
	if start < 0 || end <= start {
		t.Fatalf("section %q not found in email body", section)
	}
	if !strings.Contains(body[start:end], entity) {
		t.Fatalf("entity %q not found in %q section of email body", entity, section)
	}
}

func assertEntityNotInHTMLRange(t *testing.T, body string, start, end int, entity, section string) {
	t.Helper()
	if start < 0 || end <= start {
		t.Fatalf("section %q not found in email body", section)
	}
	if strings.Contains(body[start:end], entity) {
		t.Fatalf("entity %q should not appear in %q section of email body", entity, section)
	}
}

func AssertKafkaEmailBodyReminderNumberInSection(
	t *testing.T,
	message dbkafka.Message,
	sectionHeading, nextSectionHeading, entityName string,
	reminderNumber int,
) {
	t.Helper()

	body := kafkaEmailBody(t, message)
	start, end := emailHTMLSectionRange(body, sectionHeading, nextSectionHeading)
	if start < 0 || end <= start {
		t.Fatalf("section %q not found in email body", sectionHeading)
	}
	section := body[start:end]
	marker := fmt.Sprintf(">#%d</span>%s", reminderNumber, entityName)
	if !strings.Contains(section, marker) {
		t.Fatalf("reminder #%d for entity %q not found in %q section of email body", reminderNumber, entityName, sectionHeading)
	}
}

func AssertKafkaEmailBodyLastReminderOnly(
	t *testing.T,
	message dbkafka.Message,
	entityName string,
	reminderNumber int,
) {
	t.Helper()

	body := kafkaEmailBody(t, message)
	if !strings.Contains(body, "Final Reminder:") {
		t.Fatalf("email body missing Final Reminder title prefix")
	}
	AssertKafkaEmailBodyEntityInSection(t, message, lastReminderAlertsSection, "", entityName)
	AssertKafkaEmailBodyEntityAbsentFromSection(t, message, "New Alerts", "Reminder Alerts", entityName)
	AssertKafkaEmailBodyEntityAbsentFromSection(t, message, "Reminder Alerts", lastReminderAlertsSection, entityName)
	if reminderNumber > 0 {
		AssertKafkaEmailBodyReminderNumberInSection(t, message, lastReminderAlertsSection, "", entityName, reminderNumber)
	}
}
