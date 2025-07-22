package roiReport

import (
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"strconv"
)

type Response struct {
	LogSourceId         string
	DestinationId       string
	Incoming            string
	Outgoing            string
	ReductionPercentage string
}

func fetchPaginatedAggregate(ctx context.Context, tenantId, query string, groupBy []string, aggregations []os.AggregationFunction) ([]os.AggResponse, error) {
	var allResponses []os.AggResponse
	var after map[string]any

	for {
		responses, nextAfter, err := os.CompositePaginatedAggregate(ctx, os.GetClient(), 200, os.StatisticsIndexAlias(tenantId)+"*", query, groupBy, aggregations, after)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
			return nil, err
		}

		allResponses = append(allResponses, responses...)
		if nextAfter == nil {
			break
		}
		after = nextAfter
	}
	return allResponses, nil
}

func calculateReductionPercentage(incoming, outgoing string) (string, error) {
	if incoming == "" {
		return "0.00", nil
	}

	incomingFloat, err := strconv.ParseFloat(incoming, 64)
	if err != nil {
		return "", fmt.Errorf("error parsing incoming value: %w", err)
	}

	outgoingFloat, err := strconv.ParseFloat(outgoing, 64)
	if err != nil {
		return "", fmt.Errorf("error parsing outgoing value: %w", err)
	}

	reductionPercentage := ((incomingFloat - outgoingFloat) / incomingFloat) * 100
	return fmt.Sprintf("%.2f", reductionPercentage), nil
}

func getAggStatsForLogSourcePaginated(ctx context.Context, startTime, endTime, tenantId string) (map[string]string, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching aggregate stats for log sources", zap.String("startTime", startTime), zap.String("endTime", endTime))

	query := statistics.AddDateRange(`tags.component_name: "ingestion" AND name: "total_events_delivered"`, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword"}
	aggregations := []os.AggregationFunction{{Name: "sum_value", Function: "sum", Field: "counter.value"}}

	allResponses, err := fetchPaginatedAggregate(ctx, tenantId, query, groupBy, aggregations)
	if err != nil {
		return nil, err
	}

	lsIdToStatsMap := make(map[string]string)
	for _, resp := range allResponses {
		lsIdToStatsMap[resp.Key["tags.db_event_source_id.keyword"].(string)] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
	}
	return lsIdToStatsMap, nil
}

func getAggStatsForLogSourceToDestinationPaginated(ctx context.Context, startTime, endTime, tenantId string) ([]Response, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching destination stats for log sources", zap.String("startTime", startTime), zap.String("endTime", endTime))

	query := statistics.AddDateRange(`tags.component_name: "dispenser" AND name: "total_events_delivered"`, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword", "tags.destination_id.keyword"}
	aggregations := []os.AggregationFunction{{Name: "sum_value", Function: "sum", Field: "counter.value"}}

	allResponses, err := fetchPaginatedAggregate(ctx, tenantId, query, groupBy, aggregations)
	if err != nil {
		return nil, err
	}

	lsIdToStatsMap, err := getAggStatsForLogSourcePaginated(ctx, startTime, endTime, tenantId)
	if err != nil {
		return nil, err
	}

	var queryResponse []Response
	for _, resp := range allResponses {
		lsId := resp.Key["tags.db_event_source_id.keyword"].(string)
		destId := resp.Key["tags.destination_id.keyword"].(string)
		outgoing := fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
		incoming := lsIdToStatsMap[lsId]

		reductionPercentage, err := calculateReductionPercentage(incoming, outgoing)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error calculating reduction percentage", zap.Error(err))
			return nil, err
		}

		queryResponse = append(queryResponse, Response{
			LogSourceId:         lsId,
			DestinationId:       destId,
			Incoming:            incoming,
			Outgoing:            outgoing,
			ReductionPercentage: reductionPercentage,
		})
	}
	return queryResponse, nil
}
