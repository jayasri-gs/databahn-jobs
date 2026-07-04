package utils

import (
	"fmt"
	"sync"
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
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
	return getDynamicTopic(topicPrefix, timeNow, 0, maxTopics)
}

func GetDynamicTopicByMaxNumber(topicPrefix string, maxTopics int64) string {
	return getDynamicTopic(topicPrefix, time.Now().UnixMilli(), 0, maxTopics)
}

// GetDynamicTopicByMinAndMaxNumber returns a dynamic topic name with a minimum and maximum number of topics
// min is inclusive, max is exclusive
func GetDynamicTopicByMinAndMaxNumber(topicPrefix string, minTopic, maxTopics int64) string {
	return getDynamicTopic(topicPrefix, time.Now().UnixMilli(), minTopic, maxTopics)
}

func getDynamicTopic(topicPrefix string, timeNow, minTopic, maxTopics int64) string {
	if topicPrefix != "" {
		topicPrefix = fmt.Sprintf("%s.", topicPrefix)
	}
	if maxTopics <= minTopic {
		logger.GetLogger().Warn("minTopic >= maxTopics, resetting minTopic to 0",
			zap.String("topicPrefix", topicPrefix), zap.Int64("minTopic", minTopic), zap.Int64("maxTopics", maxTopics))
		if maxTopics < 1 {
			maxTopics = 1
		}
		minTopic = 0
	}
	suffix := minTopic + timeNow%(maxTopics-minTopic)
	return fmt.Sprintf("%s%d", topicPrefix, suffix)
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
