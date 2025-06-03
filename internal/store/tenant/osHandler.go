package tenant

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"io"
	"strings"
)

func GetIngestionByTenantId(ctx context.Context, tId uuid.UUID, startTime, endTime string) (float64, float64, error) {
	totalIngestion, err := getTotalEventsIngestedByTenantId(ctx, tId, startTime, endTime)
	if err != nil {
		return 0, 0, err
	}

	totalDataIngested, err := getTotalDataIngestedByTenantId(ctx, tId, startTime, endTime)
	if err != nil {
		return 0, 0, err
	}

	return totalIngestion, totalDataIngested, nil
}

func getTotalEventsIngestedByTenantId(ctx context.Context, tId uuid.UUID, startTime, endTime string) (float64, error) {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	response, err := statistics.GetStatsSum(ctx, q, tId, startTime, endTime)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while getting total events ingested", zap.Error(err))
		return 0, err
	}
	totalIngestion := response.Sum
	return totalIngestion, nil
}

func getTotalDataIngestedByTenantId(ctx context.Context, tId uuid.UUID, startTime, endTime string) (float64, error) {
	q := `tags.component_name: "storage" AND name: "total_data_received"`
	response, err := statistics.GetStatsSum(ctx, q, tId, startTime, endTime)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while getting total events ingested", zap.Error(err))
		return 0, err
	}
	totalIngestion := response.Sum
	return totalIngestion, nil
}

func ExecuteAggQuery(ctx context.Context, client *opensearch.Client, q, aggBy, startTime, endTime string) (map[string]any, error) {
	query := statistics.AddDateRange(q, startTime, endTime)
	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(aggBy, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, os.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
		return nil, err
	}

	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while unmarshalling response", zap.Error(err))
		return nil, err
	}
	aggObj := statistics.NewAggregateResponse(resp)
	logger.GetLoggerWithContext(ctx).Info("got stats response for query", zap.Any("q", q))
	return aggObj.Agg, err
}
