package destination

import (
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func getAggStatsForDestinationToLogSourcePaginated(ctx context.Context, startTime string, endTime string, tenantId string) (map[string]map[string]string, error) {

	logging.GetLoggerWithContext(ctx).Info("Fetching logsource stats for destination", zap.String("startTime", startTime), zap.String("endTime", endTime))
	q := `tags.component_name: "dispenser" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	groupBy := []string{"tags.destination_id.keyword", "tags.db_event_source_id.keyword"}
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
	dstIdToLogSourceStatsMap := make(map[string]map[string]string)
	for _, resp := range allResponses {
		dstId := resp.Key["tags.destination_id.keyword"].(string)
		lsId := resp.Key["tags.db_event_source_id.keyword"].(string)
		if _, exists := dstIdToLogSourceStatsMap[dstId]; !exists {
			dstIdToLogSourceStatsMap[dstId] = make(map[string]string)
		}
		dstIdToLogSourceStatsMap[dstId][lsId] = fmt.Sprintf("%v", resp.Values["sum_value"].(float64))
	}
	return dstIdToLogSourceStatsMap, nil
}
