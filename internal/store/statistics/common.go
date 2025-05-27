package statistics

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"io"
	"strings"

	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func BuildNextAggregation(termFields []string, i int) NestedAgg {
	if i == len(termFields) {
		nextAgg := NestedAgg{}
		sumValue := SumValue{}
		sumValue.Sum.Field = ES_COUNTER_VALUE_FIELD
		nextAgg.SumValue = &sumValue
		return nextAgg
	}

	nextAggs := NestedAgg{}
	groupByAgg := GroupByAgg{}
	groupByAgg.Terms.Field = termFields[i]
	groupByAgg.Terms.Size = 200
	nextAggs.GroupByAgg = &groupByAgg
	nextAggs.GroupByAgg.NestedAgg = BuildNextAggregation(termFields, i+1)
	return nextAggs
}

func GetStatsSum(ctx context.Context, q string, tenantId uuid.UUID, startTime string, endTime string) (SumResponse, error) {
	query := AddDateRange(q, startTime, endTime)

	searchBody := &SumQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query
	searchBody.Aggs.SumValue.Sum.Field = ES_COUNTER_VALUE_FIELD
	searchResponse, err := os.MakeSearchCall(ctx, os.StatisticsIndexAlias(tenantId.String()), &searchBody, os.GetClient())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
		return SumResponse{}, err
	}
	bodyContent, err := io.ReadAll(searchResponse.Body)

	resp := &SumQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := NewSumResponse(resp)
	return aggObj, err
}

func GetStatsAggregate(ctx context.Context, q string, tenantId uuid.UUID, agg string, startTime string, endTime string) (AggregateResponse, error) {
	query := AddDateRange(q, startTime, endTime)

	searchBody := &AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, os.StatisticsIndexAlias(tenantId.String()), &searchBody, os.GetClient())

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
		return AggregateResponse{}, err
	}
	bodyContent, err := io.ReadAll(searchResponse.Body)

	resp := &AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := NewAggregateResponse(resp)
	return aggObj, err
}
