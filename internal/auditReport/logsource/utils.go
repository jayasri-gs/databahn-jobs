package logsource

import (
	"context"
	"fmt"
	"math"
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

// getAggStorageBytesReceivedPerSource sums storage/total_data_received per log source (matches backend LogSourceImpl incoming data).
func getAggStorageBytesReceivedPerSource(ctx context.Context, startTime, endTime, tenantId string) (map[string]string, error) {
	q := `tags.component_name: "storage" AND name: "total_data_received"`
	return compositeSumByLogSourceID(ctx, startTime, endTime, tenantId, q)
}

// getAggDispenserBytesDeliveredPerSourceAndDestination sums dispenser/total_bytes_delivered per log source and destination
// (same grouping as destination event counts; avoids one cumulative total across all destinations).
func getAggDispenserBytesDeliveredPerSourceAndDestination(ctx context.Context, startTime, endTime, tenantId string) (map[string]map[string]string, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching dispenser byte stats per destination for log sources", zap.String("startTime", startTime), zap.String("endTime", endTime))
	q := `tags.component_name: "dispenser" AND name: "total_bytes_delivered"`
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
			logging.GetLoggerWithContext(ctx).Error("error while querying dispenser byte stats", zap.Error(err))
			return nil, err
		}

		allResponses = append(allResponses, responses...)

		if nextAfter == nil {
			break
		}
		after = nextAfter
	}
	out := make(map[string]map[string]string)
	for _, resp := range allResponses {
		lsID := resp.Key["tags.db_event_source_id.keyword"].(string)
		destID := resp.Key["tags.destination_id.keyword"].(string)
		if _, exists := out[lsID]; !exists {
			out[lsID] = make(map[string]string)
		}
		out[lsID][destID] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
	}
	return out, nil
}

func compositeSumByLogSourceID(ctx context.Context, startTime, endTime, tenantId, baseQuery string) (map[string]string, error) {
	logging.GetLoggerWithContext(ctx).Info("Fetching byte sum per log source", zap.String("startTime", startTime), zap.String("endTime", endTime))
	query := statistics.AddDateRange(baseQuery, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword"}
	aggregations := []os.AggregationFunction{
		{Name: "sum_value", Function: "sum", Field: "counter.value"},
	}

	var allResponses []os.AggResponse
	var after map[string]any

	for {
		responses, nextAfter, err := os.CompositePaginatedAggregate(ctx, os.GetClient(), 200, os.StatisticsIndexAlias(tenantId)+"*", query, groupBy, aggregations, after)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while querying statistics for bytes", zap.Error(err))
			return nil, err
		}
		allResponses = append(allResponses, responses...)
		if nextAfter == nil {
			break
		}
		after = nextAfter
	}
	out := make(map[string]string)
	for _, resp := range allResponses {
		lsID := resp.Key["tags.db_event_source_id.keyword"].(string)
		out[lsID] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
	}
	return out, nil
}

// bytesToReadableString formats byte totals with adaptive 1024^n units and two decimals (e.g. 776.15 MB), matching UI-style display.
func bytesToReadableString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	b, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return raw
	}
	if b < 0 {
		return raw
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	if b < 1024 {
		return fmt.Sprintf("%.0f B", b)
	}
	exp := int(math.Log(b) / math.Log(1024))
	if exp < 0 {
		exp = 0
	}
	if exp >= len(units) {
		exp = len(units) - 1
	}
	val := b / math.Pow(1024, float64(exp))
	return fmt.Sprintf("%.2f %s", val, units[exp])
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
