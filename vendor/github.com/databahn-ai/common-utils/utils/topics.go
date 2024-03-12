package utils

import (
	"fmt"
	"sync"
	"time"
)

var maxTopics int64
var syncMaxTopics sync.Once

func setMaxDynamicTopics() {
	numOfTopics := GetEnvInt("NUMBER_OF_OUTPUT_TOPICS", 1)
	if numOfTopics < 1 {
		numOfTopics = 1
	}
	maxTopics = int64(numOfTopics)
}

func getDynamicTopicSuffix(topicPrefix string, unit *int64) string {
	syncMaxTopics.Do(setMaxDynamicTopics)
	var timeNow int64
	if unit != nil {
		timeNow = *unit
	} else {
		timeNow = time.Now().UnixMilli()
	}
	return getDynamicTopic(topicPrefix, timeNow, maxTopics)
}

func GetDynamicTopicByMaxNumber(topicPrefix string, maxTopics int64) string {
	return getDynamicTopic(topicPrefix, time.Now().UnixMilli(), maxTopics)
}

func getDynamicTopic(topicPrefix string, timeNow, maxTopics int64) string {
	if topicPrefix != "" {
		topicPrefix = fmt.Sprintf("%s.", topicPrefix)
	}
	return fmt.Sprintf("%s%d", topicPrefix, timeNow%maxTopics)
}

func GetDynamicTopicSuffix() string {
	return getDynamicTopicSuffix("", nil)
}

func GetDynamicTopicName(topic string) string {
	return getDynamicTopicSuffix(topic, nil)
}

func GetDynamicTopicByTime(topic string, customTime int64) string {
	return getDynamicTopicSuffix(topic, &customTime)
}
