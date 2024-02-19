package kafkaquery

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"strconv"
	"sync/atomic"
	"time"
)

type CountConsumer struct {
	Count           atomic.Int64
	FirstDetectedAt atomic.Int64
	LastDetectedAt  atomic.Int64
	LastRead        atomic.Int64
	Query           *Query
	PrintTicker     *time.Ticker
	Quit            chan bool
	WaitDuration    time.Duration
}

func NewCountConsumer(query *Query, minutesToWait int) *CountConsumer {
	ticker := time.NewTicker(1 * time.Minute)
	d := time.Duration(minutesToWait) * time.Minute
	q := make(chan bool, 1)
	consumer := &CountConsumer{
		PrintTicker:  ticker,
		Query:        query,
		Quit:         q,
		WaitDuration: d,
	}
	go consumer.PrintCounts()
	return consumer
}

func (c *CountConsumer) PrintCounts() {
	for {
		select {
		case <-c.PrintTicker.C:
			nowMillis := time.Now().UnixMilli()
			logger.GetLogger().Info("count so far", zap.Int64("count", c.Count.Load()))
			if nowMillis-c.LastRead.Load() > c.WaitDuration.Milliseconds() {
				logger.GetLogger().Info("quitting after not reading message", zap.Duration("wait_duration", c.WaitDuration), zap.Int64("last seen", c.LastDetectedAt.Load()))
				c.Stop()
			}
		}
	}
}

func (c *CountConsumer) Stop() {
	c.Quit <- true
	c.PrintTicker.Stop()
}

func (c *CountConsumer) Process(message *kafka.ConsumedMessage[string, []byte]) error {
	headers := make(map[string]string)
	for _, header := range message.Headers {
		headers[header.Key] = string(header.Value)
	}
	nowMillis := time.Now().UnixMilli()
	c.LastRead.Store(nowMillis)
	if c.Query.Criteria.Evaluate(headers) {
		if c.FirstDetectedAt.Load() == 0 {
			c.FirstDetectedAt.Store(nowMillis)
		}
		c.Count.Add(1)
		c.LastDetectedAt.Store(nowMillis)
	}
	if c.LastDetectedAt.Load() != 0 && nowMillis-c.LastDetectedAt.Load() > c.WaitDuration.Milliseconds() {
		logger.GetLogger().Info("quitting after not detecting message", zap.Duration("wait_duration", c.WaitDuration), zap.Int64("first seen", c.FirstDetectedAt.Load()))
		c.Stop()
	}
	return nil
}

func (c *CountConsumer) KeyDeserialize(b []byte) (string, error) {
	return string(b), nil
}
func (c *CountConsumer) ValueDeserialize(b []byte) ([]byte, error) {
	return b, nil
}

func Start(ctx context.Context, brokers, query string, threadCount, waitMinutes int) {
	cluster := kafka.NewKafkaCluster("test", brokers)
	logger.GetLogger().Info("starting stats validator", zap.String("brokers", brokers), zap.Int("wait_minutes", waitMinutes), zap.Int("thread_count", threadCount), zap.String("query", query))
	q := Query{}
	err := json.Unmarshal([]byte(query), &q)
	if err != nil {
		logger.GetLogger().Panic("failed to unmarshal query", zap.Error(err))
	}
	if q.Id == "" {
		logger.GetLogger().Panic("id is required")
	}
	if q.Criteria == nil {
		logger.GetLogger().Panic("criteria is required")
	}
	if len(q.Topics) == 0 {
		logger.GetLogger().Panic("topics are required")
	}

	logger.GetLogger().Info("running query", zap.Any("query", q))
	process := "stats_validator" + q.Id
	consumerConf := kafka.ConsumerConfig{
		Name:        process,
		GroupId:     process + uuid.New().String(),
		NoOfThreads: threadCount,
		Topics:      q.Topics,
		OffsetReset: kafka.EarliestOffset,
	}
	consumer := NewCountConsumer(&q, waitMinutes)
	var cc kafka.DetailedKafkaConsumer[string, []byte] = consumer
	err = kafka.NewDetailedConsumer(*cluster, ctx, consumerConf, cc)
	if err != nil {
		logger.GetLogger().Panic("failed to create consumer", zap.Error(err))
	} else {
		logger.GetLogger().Info("consumer created")
	}
	<-consumer.Quit
	logger.GetLogger().Info("quitting", zap.Duration("wait_duration", consumer.WaitDuration), zap.Int64("last seen", consumer.LastDetectedAt.Load()),
		zap.Int64("last read", consumer.LastRead.Load()))
	logger.GetLogger().Info("final count", zap.Int64("count", consumer.Count.Load()))
}

type Query struct {
	Id       string    `json:"id"`
	Criteria *Criteria `json:"criteria"`
	Topics   []string  `json:"topics"`
}

type Criteria struct {
	HeaderKey     string      `json:"header_key"`
	HeaderValue   string      `json:"header_value"`
	Operator      string      `json:"operator"`
	GroupOperator string      `json:"group_operator"`
	Children      []*Criteria `json:"children"`
}

func (h *Criteria) Evaluate(headers map[string]string) bool {
	if h.Children == nil {
		return h.EvaluateSingle(headers)
	}
	switch h.GroupOperator {
	case "AND":
		for _, child := range h.Children {
			if !child.Evaluate(headers) {
				return false
			}
		}
		return true
	case "OR":
		for _, child := range h.Children {
			if child.Evaluate(headers) {
				return true
			}
		}
		return false
	default:
		logger.GetLogger().Error("unsupported group operator", zap.String("group_operator", h.GroupOperator))
		return false
	}
}

func (h *Criteria) EvaluateSingle(headers map[string]string) bool {
	actualVal, ok := headers[h.HeaderKey]
	if !ok {
		return false
	}
	switch h.Operator {
	case "==":
		return actualVal == h.HeaderValue
	case "!=":
		return actualVal != h.HeaderValue
	case ">":
		actualNumber, err := strconv.ParseInt(actualVal, 10, 64)
		if err != nil {
			logger.GetLogger().Error("failed to parse header value as number", zap.String("header_key", h.HeaderKey),
				zap.String("header_value", actualVal), zap.Error(err))
			return false
		}
		inputNumber, err := strconv.ParseInt(h.HeaderValue, 10, 64)
		if err != nil {
			logger.GetLogger().Error("failed to parse header value as number", zap.String("header_key", h.HeaderKey),
				zap.String("header_value", h.HeaderValue), zap.Error(err))
			return false
		}
		return actualNumber > inputNumber
	case "<":
		actualNumber, err := strconv.ParseInt(actualVal, 10, 64)
		if err != nil {
			logger.GetLogger().Error("failed to parse header value as number", zap.String("header_key", h.HeaderKey),
				zap.String("header_value", actualVal), zap.Error(err))
			return false
		}
		inputNumber, err := strconv.ParseInt(h.HeaderValue, 10, 64)
		if err != nil {
			logger.GetLogger().Error("failed to parse header value as number", zap.String("header_key", h.HeaderKey),
				zap.String("header_value", h.HeaderValue), zap.Error(err))
			return false
		}
		return actualNumber < inputNumber

	case ">=":
		actualNumber, err := strconv.ParseInt(actualVal, 10, 64)
		if err != nil {
			logger.GetLogger().Error("failed to parse header value as number", zap.String("header_key", h.HeaderKey),
				zap.String("header_value", actualVal), zap.Error(err))
			return false
		}
		inputNumber, err := strconv.ParseInt(h.HeaderValue, 10, 64)
		if err != nil {
			logger.GetLogger().Error("failed to parse header value as number", zap.String("header_key", h.HeaderKey),
				zap.String("header_value", h.HeaderValue), zap.Error(err))
			return false
		}
		return actualNumber >= inputNumber
	case "<=":
		actualNumber, err := strconv.ParseInt(actualVal, 10, 64)
		if err != nil {
			logger.GetLogger().Error("failed to parse header value as number", zap.String("header_key", h.HeaderKey),
				zap.String("header_value", actualVal), zap.Error(err))
			return false
		}
		inputNumber, err := strconv.ParseInt(h.HeaderValue, 10, 64)
		if err != nil {
			logger.GetLogger().Error("failed to parse header value as number", zap.String("header_key", h.HeaderKey),
				zap.String("header_value", h.HeaderValue), zap.Error(err))
			return false
		}
		return actualNumber <= inputNumber
	default:
		logger.GetLogger().Error("unsupported operator", zap.String("operator", h.Operator))
		return false
	}
}

/*
-wait_minutes=1-brokers="localhost:9092"-thread_count=4-query='{"id":"local44","topics":["db.raw.cloud"],"criteria":{"group_operator":"AND","children":[{"header_key":"db_event_source_id","header_value":"ff12b270-b9c7-487f-9b8a-0eebd6199e51","operator":"=="},{"header_key":"db_edge_ts","header_value":"1708265390000","operator":">"},{"header_key":"db_edge_ts","header_value":"1708265700000","operator":"<="}]}}'
*/

/*
{
    "id": "local44",
    "topics": [
        "db.raw.cloud"
    ],
    "criteria": {
        "group_operator": "AND",
        "children": [
            {
                "group_operator": "OR",
                "children": [
                    {
                        "header_key": "db_event_source_id",
                        "header_value": "1612b270-b9c7-487f-9b8a-0eebd6199e51",
                        "operator": "=="
                    },
                    {
                        "header_key": "db_event_source_id",
                        "header_value": "1612b270-b9c7-487f-9b8a-0eebd6199e52",
                        "operator": "=="
                    }
                ]
            },
            {
                "header_key": "db_edge_ts",
                "header_value": "1708232100000",
                "operator": ">="
            },
            {
                "header_key": "db_edge_ts",
                "header_value": "1708232400000",
                "operator": "<="
            }
        ]
    }
}
*/
