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
	query = AddTenantId(query, tenantId)
	conf := os.GetConf()
	//conf.Url = "https://localhost:9201"
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return SumResponse{}, err
	}

	searchBody := &SumQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query
	searchBody.Aggs.SumValue.Sum.Field = ES_COUNTER_VALUE_FIELD
	searchResponse, err := os.MakeSearchCall(ctx, conf.StatisticsIndexAlias(tenantId.String()), &searchBody, client)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
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
	query = AddTenantId(query, tenantId)
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return AggregateResponse{}, err
	}

	searchBody := &AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatisticsIndexAlias(tenantId.String()), &searchBody, client)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return AggregateResponse{}, err
	}
	bodyContent, err := io.ReadAll(searchResponse.Body)

	resp := &AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := NewAggregateResponse(resp)
	return aggObj, err
}

func GetAllTenantsStatsAggregate(ctx context.Context, q string, agg string, startTime string, endTime string) (AggregateResponse, error) {
	query := AddDateRange(q, startTime, endTime)
	conf := os.GetConf()
	//conf.Url = "https://localhost:7020"
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return AggregateResponse{}, err
	}

	searchBody := &AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return AggregateResponse{}, err
	}
	bodyContent, err := io.ReadAll(searchResponse.Body)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while reading body", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return AggregateResponse{}, err
	}
	resp := &AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := NewAggregateResponse(resp)
	return aggObj, err
}
