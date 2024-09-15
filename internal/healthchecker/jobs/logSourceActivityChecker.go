package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	source "github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
	"io"
	"strconv"
	"strings"
	"time"
)

func getAggStatsForDestinations(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {
	q := `tags.component_name: "dispenser" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	agg := "tags.destination_id.keyword"
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return statistics.AggregateResponse{}, err
	}

	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return statistics.AggregateResponse{}, err
	}
	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewAggregateResponse(resp)
	logging.GetLoggerWithContext(ctx).Info("got response from statistics store")
	return aggObj, err
}

func getAggStatsForLogSource(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	agg := "tags.db_event_source_id.keyword"
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return statistics.AggregateResponse{}, err
	}

	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return statistics.AggregateResponse{}, err
	}
	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewAggregateResponse(resp)
	logging.GetLoggerWithContext(ctx).Info("got response from statistics store")
	return aggObj, err
}

func checkInactivityAlertExistsForGivenLogSources(ctx context.Context, logsources []string) ([]statistics.AlertDocument, error) {
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return nil, err
	}
	q := `dismissed:false AND functionalityEntityId:` + "(" + strings.Join(logsources, " OR ") + ")" + ` AND functionalityType:` + alerts_common.LogSourceStatsNotReceived

	res, err := os.Search(ctx, client, common.AlertsIndex, q)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", common.AlertsIndex))
		return nil, err
	}

	var alerts []statistics.AlertDocument
	decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
	err = decoder.Decode(res)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while decoding response", zap.Error(err))
		return alerts, err
	}
	return alerts, nil
}

func AlertForDestinationInactivity(ctx context.Context) error {
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("Handling alerts for destination logSources")

	//get agg stats by event source - returns all destination which are reporting stats from last 15 minutes
	endTime := time.Now()
	startTime := endTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.DestinationDeliveryCheckerTime)))
	aggObj, err := getAggStatsForDestinations(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
		return err
	}
	var destinationStatsReceived []string
	for key, value := range aggObj.Agg {
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
			destinationStatsReceived = append(destinationStatsReceived, key)
		}
	}

	startTimeHistorical := endTime.Add(-time.Hour * 24 * 7)
	historicalStats, err := getAggStatsForDestinations(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(startTimeHistorical.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
		return err
	}
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

	// getting logSources which are not active but did not report stats in last 15 minutes
	var alertToBeRaisedDispenser []destination.Destination
	checkStatus := []string{healthchecker.StatusDisabled, healthchecker.StatusDeleted, healthchecker.StatusCreated, healthchecker.StatusInactive}
	err = config.GetDB().Model(&destination.Destination{}).Where("id not in ? and status not in ? AND id in ?", destinationStatsReceived, checkStatus, destinationStats7Days).Find(&alertToBeRaisedDispenser).Debug().Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting active logSources not receiving stats", zap.Error(err))
		return err
	}

	//creating alertEntityArray for all logSources for which alert needs to be raised
	var logsourcesEntityArray []alerts_common.AlertEntityObject
	var silentLogsources []string
	for _, ls := range alertToBeRaisedDispenser {
		var temp alerts_common.AlertEntityObject
		temp.EntityName = ls.Name
		temp.EntityId = ls.ID
		temp.EntityTenantUUId = ls.TenantID
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
	return nil
}

func AlertForLogSourceInactivity(ctx context.Context) error {
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("Handling alerts for inactive logSources")

	//get agg stats by event source - returns all logsources which are reporting stats from last 15 minutes
	endTime := time.Now()
	startTime := endTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.LogSourceActivityCheckerTime)))
	aggObj, err := getAggStatsForLogSource(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
		return err
	}
	var logsourceIdsStatsReceived []string
	for key, value := range aggObj.Agg {
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
			logsourceIdsStatsReceived = append(logsourceIdsStatsReceived, key)
		}
	}

	// getting logSources which are not active but did not report stats in last 7 days
	startTimehistorical := endTime.Add(-time.Hour * 24 * 7)
	historicalIds, err := getAggStatsForLogSource(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(startTimehistorical.UnixMilli())))
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

	// getting logSources which are not active but did not report stats in last 15 minutes
	var alertToBeRaisedLogSources []source.Source // array of ids not receiving stats
	checkStatus := []string{healthchecker.StatusDisabled, healthchecker.StatusDeleted, healthchecker.StatusCreated, healthchecker.StatusInactive}
	err = config.GetDB().Model(&source.Source{}).Where("id not in ? and status not in ? AND id in ?", logsourceIdsStatsReceived, checkStatus, historicalIds).Find(&alertToBeRaisedLogSources).Debug().Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting active logSources not receiving stats", zap.Error(err))
		return err
	}

	//creating alertEntityArray for all logSources for which alert needs to be raised
	var logsourcesEntityArray []alerts_common.AlertEntityObject
	var silentLogsources []string
	for _, ls := range alertToBeRaisedLogSources {
		var temp alerts_common.AlertEntityObject
		temp.EntityName = ls.Name
		temp.EntityId = ls.ID
		temp.EntityTenantUUId = ls.TenantID
		logsourcesEntityArray = append(logsourcesEntityArray, temp)

		silentLogsources = append(silentLogsources, ls.ID.String())
	}

	// check if any logsource whose stats came back but alert exists
	alerts, err := checkInactivityAlertExistsForGivenLogSources(ctx, logsourceIdsStatsReceived)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while checking inactivity alert exists", zap.Error(err))
		return err
	}

	// get alerts array for dismissal
	var toDismissAlerts []alerts_common.AlertEntityObject
	for _, alert := range alerts {
		var temp alerts_common.AlertEntityObject
		temp.EntityName = alert.FunctionalityEntityName
		temp.EntityId = utils.UUIDFromStringOrNil(alert.FunctionalityEntityId)
		temp.EntityTenantUUId = utils.UUIDFromStringOrNil(alert.TenantId)
		toDismissAlerts = append(toDismissAlerts, temp)
	}

	// send dismiss alerts to change flag
	if len(toDismissAlerts) > 0 {
		err = helper.SendAlertToControlPlane(ctx, toDismissAlerts, fmt.Sprintf(alerts_common.LogSourceStatsNotReceivedTitle, healthchecker.LogSourceActivityCheckerTime), fmt.Sprintf(alerts_common.LogSourceStatsNotReceivedMessage, healthchecker.LogSourceActivityCheckerTime), alerts_common.LogSourceStatsNotReceived, alerts_common.LogSourceFunctionality, alerts_common.SevereAlert, alerts_common.AlertAutoResolved, true, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while dismissing alerts for logSource activity check", zap.Error(err))
			return err
		}
	}

	// update logsource mark silent
	err = config.GetDB().Model(&source.Source{}).Where("id in ? ", silentLogsources).Updates(map[string]interface{}{"reputation": common.SILENT}).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while marking log sources as silent", zap.Error(err))
		return err
	}

	// raise alert and save it to opensearch
	if len(logsourcesEntityArray) > 0 {
		err = helper.SendAlertToControlPlane(ctx, logsourcesEntityArray, fmt.Sprintf(alerts_common.LogSourceStatsNotReceivedTitle, healthchecker.LogSourceActivityCheckerTime), fmt.Sprintf(alerts_common.LogSourceStatsNotReceivedMessage, healthchecker.LogSourceActivityCheckerTime), alerts_common.LogSourceStatsNotReceived, alerts_common.LogSourceFunctionality, alerts_common.SevereAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while raising alert for logSource activity check", zap.Error(err))
			return err
		}
	}
	return nil
}
