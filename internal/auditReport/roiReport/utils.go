package roiReport

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type Response struct {
	LogSourceId              string
	DestinationId            string
	IncomingBytes            string
	OutgoingBytes            string
	ByteReductionPercentage  string
	IncomingEvents           string
	OutgoingEvents           string
	EventReductionPercentage string
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

	reductionPercentage := math.Max(0, ((incomingFloat-outgoingFloat)/incomingFloat)*100)
	return fmt.Sprintf("%.2f", reductionPercentage), nil
}

// Incoming bytes per source: total_data_received at the storage layer
func getIncomingBytesBySource(ctx context.Context, startTime, endTime, tenantId string) (map[string]string, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching incoming bytes per source", zap.String("startTime", startTime), zap.String("endTime", endTime))

	query := statistics.AddDateRange(`name: "total_data_received" AND tags.component_name: "storage"`, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword"}
	aggregations := []os.AggregationFunction{{Name: "sum_value", Function: "sum", Field: "counter.value"}}

	allResponses, err := fetchPaginatedAggregate(ctx, tenantId, query, groupBy, aggregations)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, resp := range allResponses {
		result[resp.Key["tags.db_event_source_id.keyword"].(string)] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
	}
	return result, nil
}

// Incoming events per source: total_events_delivered at the ingestion layer
func getIncomingEventsBySource(ctx context.Context, startTime, endTime, tenantId string) (map[string]string, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching incoming events per source", zap.String("startTime", startTime), zap.String("endTime", endTime))

	query := statistics.AddDateRange(`tags.component_name: "ingestion" AND name: "total_events_delivered"`, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword"}
	aggregations := []os.AggregationFunction{{Name: "sum_value", Function: "sum", Field: "counter.value"}}

	allResponses, err := fetchPaginatedAggregate(ctx, tenantId, query, groupBy, aggregations)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, resp := range allResponses {
		result[resp.Key["tags.db_event_source_id.keyword"].(string)] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
	}
	return result, nil
}

// Outgoing bytes per source-destination pair: total_bytes_delivered at the dispenser layer
func getOutgoingBytesBySourceDestination(ctx context.Context, startTime, endTime, tenantId string) ([]os.AggResponse, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching outgoing bytes per source-destination", zap.String("startTime", startTime), zap.String("endTime", endTime))

	query := statistics.AddDateRange(`name: "total_bytes_delivered" AND tags.component_name: "dispenser"`, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword", "tags.destination_id.keyword"}
	aggregations := []os.AggregationFunction{{Name: "sum_value", Function: "sum", Field: "counter.value"}}

	return fetchPaginatedAggregate(ctx, tenantId, query, groupBy, aggregations)
}

// Outgoing events per source-destination pair: total_events_delivered at the dispenser layer
func getOutgoingEventsBySourceDestination(ctx context.Context, startTime, endTime, tenantId string) ([]os.AggResponse, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching outgoing events per source-destination", zap.String("startTime", startTime), zap.String("endTime", endTime))

	query := statistics.AddDateRange(`tags.component_name: "dispenser" AND name: "total_events_delivered"`, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword", "tags.destination_id.keyword"}
	aggregations := []os.AggregationFunction{{Name: "sum_value", Function: "sum", Field: "counter.value"}}

	return fetchPaginatedAggregate(ctx, tenantId, query, groupBy, aggregations)
}

type sourceDestKey struct {
	logSourceId   string
	destinationId string
}

func getAggStatsForLogSourceToDestinationPaginated(ctx context.Context, startTime, endTime, tenantId string) ([]Response, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching ROI stats for log sources", zap.String("startTime", startTime), zap.String("endTime", endTime))

	incomingBytesMap, err := getIncomingBytesBySource(ctx, startTime, endTime, tenantId)
	if err != nil {
		return nil, err
	}

	incomingEventsMap, err := getIncomingEventsBySource(ctx, startTime, endTime, tenantId)
	if err != nil {
		return nil, err
	}

	outgoingBytesResp, err := getOutgoingBytesBySourceDestination(ctx, startTime, endTime, tenantId)
	if err != nil {
		return nil, err
	}

	outgoingEventsResp, err := getOutgoingEventsBySourceDestination(ctx, startTime, endTime, tenantId)
	if err != nil {
		return nil, err
	}

	outgoingBytesMap := make(map[sourceDestKey]string)
	allKeys := make(map[sourceDestKey]struct{})
	for _, resp := range outgoingBytesResp {
		key := sourceDestKey{
			logSourceId:   resp.Key["tags.db_event_source_id.keyword"].(string),
			destinationId: resp.Key["tags.destination_id.keyword"].(string),
		}
		outgoingBytesMap[key] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
		allKeys[key] = struct{}{}
	}

	outgoingEventsMap := make(map[sourceDestKey]string)
	for _, resp := range outgoingEventsResp {
		key := sourceDestKey{
			logSourceId:   resp.Key["tags.db_event_source_id.keyword"].(string),
			destinationId: resp.Key["tags.destination_id.keyword"].(string),
		}
		outgoingEventsMap[key] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
		allKeys[key] = struct{}{}
	}

	var queryResponse []Response
	for key := range allKeys {
		incomingBytes := incomingBytesMap[key.logSourceId]
		outgoingBytes := outgoingBytesMap[key]
		if outgoingBytes == "" {
			outgoingBytes = "0"
		}

		incomingEvents := incomingEventsMap[key.logSourceId]
		outgoingEvents := outgoingEventsMap[key]
		if outgoingEvents == "" {
			outgoingEvents = "0"
		}

		byteReduction, err := calculateReductionPercentage(incomingBytes, outgoingBytes)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error calculating byte reduction percentage", zap.Error(err))
			return nil, err
		}

		eventReduction, err := calculateReductionPercentage(incomingEvents, outgoingEvents)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error calculating event reduction percentage", zap.Error(err))
			return nil, err
		}

		queryResponse = append(queryResponse, Response{
			LogSourceId:              key.logSourceId,
			DestinationId:            key.destinationId,
			IncomingBytes:            incomingBytes,
			OutgoingBytes:            outgoingBytes,
			ByteReductionPercentage:  byteReduction,
			IncomingEvents:           incomingEvents,
			OutgoingEvents:           outgoingEvents,
			EventReductionPercentage: eventReduction,
		})
	}
	return queryResponse, nil
}
