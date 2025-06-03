package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"reflect"
	"strconv"
	"time"
)

func getAggStatsForDestinationPaginated(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {

	logging.GetLoggerWithContext(ctx).Info("Fetching aggregate stats for destinations", zap.String("startTime", startTime), zap.String("endTime", endTime))

	query := `tags.component_name: "dispenser" AND name: "total_events_delivered"`
	query = statistics.AddDateRange(query, startTime, endTime)
	groupBy := []string{"tags.destination_id.keyword"}
	aggregations := []os.AggregationFunction{
		{Name: "sum_value", Function: "sum", Field: "counter.value"},
	}

	var allResponses []os.AggResponse
	var after map[string]any

	for {
		responses, nextAfter, err := os.CompositePaginatedAggregate(ctx, os.GetClient(), 100, os.StatsIndex+"*", query, groupBy, aggregations, after)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
			return statistics.AggregateResponse{}, err
		}

		allResponses = append(allResponses, responses...)

		if nextAfter == nil {
			break
		}
		after = nextAfter
	}

	logging.GetLoggerWithContext(ctx).Info("Number of responses received", zap.Int("count", len(allResponses)))

	aggMap := make(map[string]any)
	for _, resp := range allResponses {
		if sumValue, ok := resp.Values["sum_value"]; ok {
			aggMap[resp.Key["tags.destination_id.keyword"].(string)] = sumValue
		}
	}

	logging.GetLoggerWithContext(ctx).Info("Aggregate Response for destinations", zap.Any("aggMap", aggMap))

	return statistics.AggregateResponse{Agg: aggMap}, nil
}

func AlertForDestinationInactivity(ctx context.Context) error {
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("Handling alerts for destination logSources")

	//get agg stats by event source - returns all destination which are reporting stats from last 15 minutes
	endTime := time.Now()
	startTime := endTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.DestinationDeliveryCheckerTime)))
	aggObj, err := getAggStatsForDestinationPaginated(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
		return err
	}

	logging.GetLoggerWithContext(ctx).Info("Destination stats for last 30 minutes", zap.Any("stats", aggObj.Agg))

	var destinationStatsReceived []string
	for key, value := range aggObj.Agg {
		valueInt, ok := value.(float64)
		if !ok {
			logging.GetLoggerWithContext(ctx).Error("error while getting value of stats for destination", zap.Error(err), zap.String("type", reflect.TypeOf(value).String()))
			continue
		}
		if valueInt > 0 {
			_, err := uuid.Parse(key)
			if err != nil {
				continue
			}
			destinationStatsReceived = append(destinationStatsReceived, key)
		}
	}

	startTimeHistorical := endTime.Add(-time.Hour * 24 * 7)
	historicalStats, err := getAggStatsForDestinationPaginated(ctx, strconv.Itoa(int(startTimeHistorical.UnixMilli())), strconv.Itoa(int(startTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
		return err
	}
	logging.GetLoggerWithContext(ctx).Info("Destinations stats for last 7 days", zap.Any("historicalStats", historicalStats.Agg))

	var destinationStats7Days []string
	for key, value := range historicalStats.Agg {
		valueInt, ok := value.(float64)
		if !ok {
			logging.GetLoggerWithContext(ctx).Error("error while getting value of stats", zap.Error(err))
			return err
		}
		if valueInt > 0 {
			_, err := uuid.Parse(key)
			if err != nil {
				continue
			}
			destinationStats7Days = append(destinationStats7Days, key)
		}
	}

	var destinationIdsToAlert []string
	for _, id := range destinationStats7Days {
		if !util.Contains(destinationStatsReceived, id) {
			destinationIdsToAlert = append(destinationIdsToAlert, id)
		}
	}

	logging.GetLoggerWithContext(ctx).Info("Destination IDs to alert", zap.Any("destinationIdsToAlert", destinationIdsToAlert))

	// getting destinations which are not active but did not report stats
	var alertToBeRaisedDispenser []destination.Destination
	checkStatus := []string{healthchecker.StatusDisabled, healthchecker.StatusDeleted, healthchecker.StatusCreated, healthchecker.StatusInactive}
	err = config.GetDB().Model(&destination.Destination{}).Where("status not in ? AND id in ?", checkStatus, destinationIdsToAlert).Debug().Find(&alertToBeRaisedDispenser).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting active destinations not receiving stats", zap.Error(err))
		return err
	}

	//creating alertEntityArray for all logSources for which alert needs to be raised
	var logsourcesEntityArray []alerts_common.AlertBaseObjectV2
	var silentLogsources []string
	for _, ls := range alertToBeRaisedDispenser {
		var temp alerts_common.AlertBaseObjectV2
		temp.EntityName = ls.Name
		temp.EntityId = ls.ID
		temp.EntityTenantUUId = ls.TenantID
		temp.DataPlaneId = ls.DataPlaneId
		temp.AlertType = alerts_common.AlertTypeExternalAndExternal
		temp.ErrorCode = healthchecker.DNDW10002
		logsourcesEntityArray = append(logsourcesEntityArray, temp)

		silentLogsources = append(silentLogsources, ls.ID.String())
	}

	// raise alert and save it to opensearch
	if len(logsourcesEntityArray) > 0 {
		err = helper.SendAlertToControlPlane(ctx, logsourcesEntityArray, fmt.Sprintf(alerts_common.DestinationStatsNotReceivedTitle, healthchecker.LogSourceActivityCheckerTime), fmt.Sprintf(alerts_common.DestinationStatsNotReceivedMessage, healthchecker.LogSourceActivityCheckerTime), alerts_common.DestinationStatsNotReceived, alerts_common.DestinationFunctionality, alerts_common.SevereAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while raising alert for destination activity check", zap.Error(err))
			return err
		}
	}
	logging.GetLogger().Info("notification stats as follows for destinations inactivity", zap.Any("no_of_inactive_destinations", len(logsourcesEntityArray)), zap.Any("ids", logsourcesEntityArray))
	return nil
}
