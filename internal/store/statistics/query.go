package statistics

import (
	"fmt"
	"strconv"

	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func AddTenantId(q string, tenantId uuid.UUID) string {
	if tenantId == uuid.Nil {
		return q
	}
	tenantIdQuery := fmt.Sprintf("%s: \"%s\"", TAG_TENANT_ID, tenantId.String())
	if q == "" {
		return tenantIdQuery
	} else {
		return fmt.Sprintf("(%s) AND (%s)", tenantIdQuery, q)
	}
}

func AddDateRange(q string, startTime string, endTime string) string {
	if startTime == "" || endTime == "" {
		return q
	}
	start, err := strconv.ParseInt(startTime, 10, 64)
	if err != nil {
		logger.GetLogger().Error("Failed to parse startTime", zap.String("startTime", startTime))
		return q
	}
	end, err := strconv.ParseInt(endTime, 10, 64)
	if err != nil {
		logger.GetLogger().Error("Failed to parse endTime", zap.String("endTime", endTime))
		return q
	}

	dateRangeQuery := fmt.Sprintf("%s:[%d TO %d]", ES_TIME_FIELD, start, end)
	if q == "" {
		return dateRangeQuery
	} else {
		return fmt.Sprintf("(%s) AND (%s)", q, dateRangeQuery)
	}
}

type ErrorResponse struct {
	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

type SumQueryRequest struct {
	Size  int `json:"size"`
	Query struct {
		QueryString struct {
			Query string `json:"query"`
		} `json:"query_string"`
	} `json:"query"`
	Aggs struct {
		SumValue struct {
			Sum struct {
				Field string `json:"field"`
			} `json:"sum"`
		} `json:"sum_value"`
	} `json:"aggs"`
}

type SumQueryResponse struct {
	ErrorResponse
	Hits struct {
		Total struct {
			Value int64 `json:"value"`
		} `json:"total"`
	} `json:"hits"`
	Aggregations struct {
		SumValue struct {
			Value float64 `json:"value"`
		} `json:"sum_value"`
	} `json:"aggregations"`
}

type AggregateQueryRequest struct {
	Size  int `json:"size"`
	Query struct {
		QueryString struct {
			Query string `json:"query"`
		} `json:"query_string"`
	} `json:"query"`
	NestedAgg `json:"aggs"`
}
type GroupByAgg struct {
	Terms     Terms     `json:"terms"`
	NestedAgg NestedAgg `json:"aggs"`
}

type Terms struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}

type NestedAgg struct {
	*GroupByAgg `json:"group_by,omitempty"`
	*SumValue   `json:"sum_value,omitempty"`
}

type SumValue struct {
	Sum struct {
		Field string `json:"field"`
	} `json:"sum"`
}

type AggregateQueryResponse struct {
	ErrorResponse
	Hits struct {
		Total struct {
			Value int64 `json:"value"`
		} `json:"total"`
	} `json:"hits"`
	Aggregations struct {
		GroupBy `json:"group_by"`
	} `json:"aggregations"`
}

type GroupBy struct {
	Buckets []struct {
		Key      string  `json:"key"`
		GroupBy  GroupBy `json:"group_by,omitempty"`
		SumValue struct {
			Value float64 `json:"value"`
		} `json:"sum_value,omitempty"`
	} `json:"buckets"`
}

type HistogramQueryRequest struct {
	Size  int `json:"size"`
	Query struct {
		QueryString struct {
			Query string `json:"query"`
		} `json:"query_string"`
	} `json:"query"`
	Aggs struct {
		SumOverTime struct {
			DateHistogram struct {
				Field    string `json:"field"`
				Interval string `json:"fixed_interval"`
			} `json:"date_histogram"`
			Aggs struct {
				SumValue struct {
					Sum struct {
						Field string `json:"field"`
					} `json:"sum"`
				} `json:"sum_value"`
			} `json:"aggs"`
		} `json:"sum_over_time"`
	} `json:"aggs"`
}

type HistogramQueryResponse struct {
	ErrorResponse
	Hits struct {
		Total struct {
			Value int64 `json:"value"`
		} `json:"total"`
	} `json:"hits"`
	Aggregations struct {
		SumOverTime struct {
			Buckets []struct {
				Key         int64  `json:"key"`
				KeyAsString string `json:"key_as_string"`
				SumValue    struct {
					Value float64 `json:"value"`
				} `json:"sum_value"`
			} `json:"buckets"`
		} `json:"sum_over_time"`
	} `json:"aggregations"`
}

type AlertDocument struct {
	Class                   string `json:"_class"`
	Id                      string `json:"id"`
	Title                   string `json:"title"`
	Criticality             string `json:"criticality"`
	Message                 string `json:"message"`
	CreatedAt               int64  `json:"createdAt"`
	UpdatedAt               int64  `json:"updatedAt"`
	FirstObservedAt         int64  `json:"firstObservedAt"`
	LastObservedAt          int64  `json:"lastObservedAt"`
	TenantId                string `json:"tenantId"`
	FunctionalityType       string `json:"functionalityType"`
	Functionality           string `json:"functionality"`
	FunctionalityEntityId   string `json:"functionalityEntityId"`
	FunctionalityEntityName string `json:"functionalityEntityName"`
	Dismissed               bool   `json:"dismissed"`
	DismissedAt             int64  `json:"dismissedAt"`
	DismissedBy             string `json:"dismissedBy"`
}

func ParseAlertDocuments(osDocuments []map[string]any) ([]AlertDocument, error) {
	var alerts []AlertDocument
	decoder, err := util.CreateAlertDecoder(&alerts)
	if err != nil {
		logger.GetLogger().Error("error while creating decoder for alerts", zap.Error(err))
		return nil, err
	}
	err = decoder.Decode(osDocuments)
	if err != nil {
		logger.GetLogger().Error("error while decoding openSearch response", zap.Error(err))
		return nil, err
	}
	return alerts, nil
}

type DeviceInventoryDocument struct {
	Id         string `json:"id"`
	Hostname   string `json:"key1"`
	Key2       string `json:"key2"`
	SmallName  string `json:"key3"`
	TenantId   string `json:"tenant_id"`
	MinTime    int64  `json:"min_time"`
	MaxTime    int64  `json:"max_time"`
	SourceId   string `json:"source_id"`
	Timestamp  int64  `json:"timestamp"`
	UpdatedAt  int64  `json:"updated_at"`
	Reputation string `json:"reputation"`
}
