package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	source "github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func getAggStatsForLogSourcePaginated(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {

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
		responses, nextAfter, err := os.CompositePaginatedAggregate(ctx, os.GetClient(), 200, os.StatsIndex+"*", query, groupBy, aggregations, after)
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
			aggMap[resp.Key["tags.db_event_source_id.keyword"].(string)] = sumValue
		}
	}
	logging.GetLoggerWithContext(ctx).Info("Aggregate Response for log sources", zap.Any("aggMap", aggMap))

	return statistics.AggregateResponse{Agg: aggMap}, nil
}

func PaginatedOpenSearchCallToGetAllExistingAlerts(ctx context.Context, logsources []string) ([]statistics.AlertDocument, error) {

	logging.GetLoggerWithContext(ctx).Info("Checking for existing alerts for log sources", zap.Strings("logSources", logsources))

	if len(logsources) == 0 {
		logging.GetLoggerWithContext(ctx).Info("No log sources to check for existing alerts")
		return nil, nil
	}

	q := `dismissed:false AND functionalityEntityId:` + "(" + strings.Join(logsources, " OR ") + ")" + ` AND functionalityType:` + alerts_common.LogSourceStatsNotReceived

	var allAlerts []statistics.AlertDocument
	var searchAfter []any

	for {
		res, newSearchAfter, err := os.SearchPaginated(ctx, os.GetClient(), common.AlertsIndex, q, 200, searchAfter, []os.Sort{{Field: "updatedAt", Order: "desc"}})
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
			return nil, err
		}

		var alerts []statistics.AlertDocument
		decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
		err = decoder.Decode(res)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while decoding response", zap.Error(err))
			return allAlerts, err
		}
		allAlerts = append(allAlerts, alerts...)

		if len(res) == 0 || newSearchAfter == nil {
			break
		}
		searchAfter = newSearchAfter
	}

	logging.GetLoggerWithContext(ctx).Info("Number of existing alerts found", zap.Int("count", len(allAlerts)))

	return allAlerts, nil

}

func AlertForLogSourceInactivity(ctx context.Context) error {
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("Handling alerts for inactive logSources")

	// get agg stats by event source - returns all logsources which are reporting stats from last 15 minutes
	endTime := time.Now()
	startTime := endTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.LogSourceActivityCheckerTime)))
	aggObj, err := getAggStatsForLogSourcePaginated(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
		return err
	}
	logging.GetLoggerWithContext(ctx).Info("Fetched stats for log sources", zap.Any("stats", aggObj.Agg))
	var logsourceIdsStatsReceived []string
	for key, value := range aggObj.Agg {
		valueInt, ok := value.(float64)
		if !ok {
			logging.GetLoggerWithContext(ctx).Error("error while getting value of stats for source", zap.Error(err), zap.String("type", reflect.TypeOf(value).String()))
			continue
		}
		if valueInt > 0 {
			_, err := uuid.Parse(key)
			if err != nil {
				continue
			}
			logsourceIdsStatsReceived = append(logsourceIdsStatsReceived, key)
		}
	}

	// getting logSources which are not active but did not report stats in last 7 days
	startTimeHistorical := endTime.Add(-(time.Hour * 24 * 7))
	historicalIds, err := getAggStatsForLogSourcePaginated(ctx, strconv.Itoa(int(startTimeHistorical.UnixMilli())), strconv.Itoa(int(startTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
		return err
	}
	var statsExistsInLast7Days []string
	for key, value := range historicalIds.Agg {
		valueInt, ok := value.(float64)
		if !ok {
			logging.GetLoggerWithContext(ctx).Error("error while getting value of stats", zap.Error(err))
			return err
		}
		if valueInt > 0 {
			statsExistsInLast7Days = append(statsExistsInLast7Days, key)
		}
	}
	logging.GetLoggerWithContext(ctx).Info("Log source IDs with stats in last 7 days", zap.Any("stats", statsExistsInLast7Days))
	var sourceIdToAlert []string
	for _, id := range statsExistsInLast7Days {
		if !util.Contains(logsourceIdsStatsReceived, id) {
			sourceIdToAlert = append(sourceIdToAlert, id)
		}
	}
	logging.GetLoggerWithContext(ctx).Info("Log source IDs to alert", zap.Any("sourceIdToAlert", sourceIdToAlert))

	// getting logSources which are not active but did not report stats in last 15 minutes
	var alertToBeRaisedLogSources []source.Source // array of ids not receiving stats
	checkStatus := []string{healthchecker.StatusDisabled, healthchecker.StatusDeleted, healthchecker.StatusCreated, healthchecker.StatusInactive}
	err = config.GetDB().Model(&source.Source{}).Where("status not in ? AND id in ?", checkStatus, sourceIdToAlert).Debug().Find(&alertToBeRaisedLogSources).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting active logSources not receiving stats", zap.Error(err))
		return err
	}
	logging.GetLoggerWithContext(ctx).Info("Log sources to raise alerts before filtering", zap.Any("alertToBeRaisedLogSources", alertToBeRaisedLogSources))

	configMap, err := helper.CreateEntityAlertsConfigMapByType(config.GetDB())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while creating config map", zap.Error(err))
		return err
	}

	logging.GetLoggerWithContext(ctx).Info("Config map for log sources for interval based alerts", zap.Any("configMap", configMap))

	// creating alertEntityArray for all logSources for which alert needs to be raised
	var filteredAlertCandidates []source.Source
	for _, ls := range alertToBeRaisedLogSources {
		if configEntry, ok := configMap[fmt.Sprintf("LOG_SOURCE_%s", ls.ID.String())]; ok {
			alertThreshold := configEntry.LastCheckedTime.Add(time.Minute * time.Duration(configEntry.Interval))
			logging.GetLogger().Info("Alert threshold", zap.Time("alert_threshold", alertThreshold))
			if startTime.After(alertThreshold) {
				filteredAlertCandidates = append(filteredAlertCandidates, ls)
				logging.GetLoggerWithContext(ctx).Info("Interval-based alert kept for source", zap.String("source_id", ls.ID.String()), zap.Time("last_checked_time", configEntry.LastCheckedTime), zap.Time("alert_threshold", alertThreshold), zap.Time("start_time_short", startTime))
			} else {
				logging.GetLoggerWithContext(ctx).Info("Short-term alert discarded for source due to interval threshold not met", zap.String("source_id", ls.ID.String()), zap.Time("last_checked_time", configEntry.LastCheckedTime), zap.Time("alert_threshold", alertThreshold), zap.Time("start_time_short", startTime))
			}
		} else {
			filteredAlertCandidates = append(filteredAlertCandidates, ls)
			logging.GetLoggerWithContext(ctx).Info("No interval configured, keeping short-term alert for source", zap.String("source_id", ls.ID.String()))
		}
	}
	logging.GetLoggerWithContext(ctx).Info("Log sources after interval filtering", zap.Any("filteredAlertCandidates", filteredAlertCandidates))

	var logsourcesEntityArray []alerts_common.AlertBaseObjectV2
	var silentLogsources []string
	for _, ls := range filteredAlertCandidates {
		var temp alerts_common.AlertBaseObjectV2
		temp.EntityName = ls.Name
		temp.EntityId = ls.ID
		temp.EntityTenantUUId = ls.TenantID
		temp.DataPlaneId = ls.DataPlaneId
		temp.AlertType = alerts_common.AlertTypeExternalAndExternal
		temp.ErrorCode = healthchecker.DNDW10001
		logsourcesEntityArray = append(logsourcesEntityArray, temp)

		silentLogsources = append(silentLogsources, ls.ID.String())
	}
	logging.GetLoggerWithContext(ctx).Info("Log sources entity array for alerts", zap.Any("logsourcesEntityArray", logsourcesEntityArray))
	logging.GetLoggerWithContext(ctx).Info("Silent log sources", zap.Strings("silentLogsources", silentLogsources))

	// check if any logsource whose stats came back but alert exists
	alerts, err := PaginatedOpenSearchCallToGetAllExistingAlerts(ctx, logsourceIdsStatsReceived)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while checking inactivity alert exists", zap.Error(err))
		return err
	}
	logging.GetLoggerWithContext(ctx).Info("Existing alerts for dismissal", zap.Any("alerts", alerts))

	// get alerts array for dismissal
	var toDismissAlerts []alerts_common.AlertBaseObjectV2
	for _, alert := range alerts {
		var temp alerts_common.AlertBaseObjectV2
		temp.EntityName = alert.FunctionalityEntityName
		temp.EntityId = utils.UUIDFromStringOrNil(alert.FunctionalityEntityId)
		temp.EntityTenantUUId = utils.UUIDFromStringOrNil(alert.TenantId)
		toDismissAlerts = append(toDismissAlerts, temp)
	}
	logging.GetLoggerWithContext(ctx).Info("Alerts to dismiss for log sources inactivity", zap.Any("toDismissAlerts", toDismissAlerts))

	// send dismiss alerts to change flag
	if len(toDismissAlerts) > 0 {
		err = helper.SendAlertToControlPlane(ctx, toDismissAlerts, fmt.Sprintf(alerts_common.LogSourceStatsNotReceivedTitle, healthchecker.LogSourceActivityCheckerTime), fmt.Sprintf(alerts_common.LogSourceStatsNotReceivedMessage, healthchecker.LogSourceActivityCheckerTime), alerts_common.LogSourceStatsNotReceived, alerts_common.LogSourceFunctionality, alerts_common.SevereAlert, alerts_common.AlertAutoResolved, true, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while dismissing alerts for logSource activity check", zap.Error(err))
			return err
		}
	}

	logging.GetLoggerWithContext(ctx).Info("Log sources dismissed", zap.Any("DismissedLogSources", toDismissAlerts))

	// update logsource mark silent
	err = config.GetDB().Model(&source.Source{}).Where("id in ? ", silentLogsources).Updates(map[string]interface{}{"reputation": common.SILENT}).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while marking log sources as silent", zap.Error(err))
		return err
	}

	logging.GetLoggerWithContext(ctx).Info("Log sources marked silent", zap.Strings("silentLogSources", silentLogsources))

	// raise alert and save it to opensearch
	logging.GetLoggerWithContext(ctx).Info("Raising alert for log sources", zap.Any("logsourcesEntityArray", logsourcesEntityArray))
	if len(logsourcesEntityArray) > 0 {
		err = helper.SendAlertToControlPlane(ctx, logsourcesEntityArray, fmt.Sprintf(alerts_common.LogSourceStatsNotReceivedTitle, healthchecker.LogSourceActivityCheckerTime), fmt.Sprintf(alerts_common.LogSourceStatsNotReceivedMessage, healthchecker.LogSourceActivityCheckerTime), alerts_common.LogSourceStatsNotReceived, alerts_common.LogSourceFunctionality, alerts_common.SevereAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while raising alert for logSource activity check", zap.Error(err))
			return err
		}
	}
	logging.GetLogger().Info("notification stats as follows for log source inactivity", zap.Any("no_of_inactive_sources", len(logsourcesEntityArray)), zap.Any("ids", logsourcesEntityArray))
	return nil
}
