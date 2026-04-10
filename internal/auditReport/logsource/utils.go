package logsource

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func getAggStatsForLogSourcePaginated(ctx context.Context, startTime string, endTime string, tenantId string) (map[string]string, error) {

	logging.GetLoggerWithContext(ctx).Info("Fetching aggregate stats for log sources", zap.String("startTime", startTime), zap.String("endTime", endTime))
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword"}
	aggregations := []os.AggregationFunction{
		{Name: "sum_value", Function: "sum", Field: "counter.value"},
	}

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
	lsIdToStatsMap := make(map[string]string)
	for _, resp := range allResponses {
		lsIdToStatsMap[resp.Key["tags.db_event_source_id.keyword"].(string)] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
	}
	return lsIdToStatsMap, nil
}

func getAggStatsForLogSourceToDestinationPaginated(ctx context.Context, startTime string, endTime string, tenantId string) (map[string]map[string]string, error) {

	logging.GetLoggerWithContext(ctx).Info("Fetching destination stats for log sources", zap.String("startTime", startTime), zap.String("endTime", endTime))
	q := `tags.component_name: "dispenser" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword", "tags.destination_id.keyword"}
	aggregations := []os.AggregationFunction{
		{Name: "sum_value", Function: "sum", Field: "counter.value"},
	}

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
	lsIdToDestinaionStatsMap := make(map[string]map[string]string)
	for _, resp := range allResponses {
		lsId := resp.Key["tags.db_event_source_id.keyword"].(string)
		destId := resp.Key["tags.destination_id.keyword"].(string)
		if _, exists := lsIdToDestinaionStatsMap[lsId]; !exists {
			lsIdToDestinaionStatsMap[lsId] = make(map[string]string)
		}
		lsIdToDestinaionStatsMap[lsId][destId] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
	}
	return lsIdToDestinaionStatsMap, nil
}

// formatEventCountReadable abbreviates non-negative event counts for CSV (e.g. 259.38K, 1.5M).
// K/M/B use two decimal places on the scaled value, then trailing zeros are trimmed (e.g. 1.10B -> 1.1B).
// Empty or non-numeric input returns empty or the original string.
func formatEventCountReadable(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return raw
	}
	if n < 0 {
		return raw
	}
	if n < 1000 {
		if n == float64(int64(n)) {
			return strconv.FormatInt(int64(n), 10)
		}
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", n), "0"), ".")
	}

	var scaled float64
	var suffix string
	switch {
	case n < 1_000_000:
		scaled, suffix = n/1000, "K"
	case n < 1_000_000_000:
		scaled, suffix = n/1_000_000, "M"
	default:
		scaled, suffix = n/1_000_000_000, "B"
	}
	out := fmt.Sprintf("%.2f", scaled)
	out = strings.TrimRight(strings.TrimRight(out, "0"), ".")
	return out + suffix
}
