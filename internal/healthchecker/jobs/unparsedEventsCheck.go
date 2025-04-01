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
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
	"io"
	"strconv"
	"strings"
	"time"
)

func GetUnparsedEvents(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {

	q := `tags.component_name: "parser" AND name: "total_events_delivered" AND namespace:"parsing-service-unparsed"`
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
func AlertForUnparsedEvents(ctx context.Context) error {
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("Starting job to raise alerts for unparsed events")

	endTime := time.Now()
	startTime := endTime.Add(-time.Hour)

	logging.GetLoggerWithContext(ctx).Info("pulling stats for", zap.Time("startTime", startTime), zap.Time("endTime", endTime))

	aggObj, err := GetUnparsedEvents(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting unparsed events stats", zap.Error(err))
		return err
	}

	totalEventsObj, err := GetEventDeliveryStats(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting total events stats", zap.Error(err))
		return err
	}
	// get source id to unparsed event count mapping
	sourceIdToUnparsedEventCount := make(map[string]float64)
	for sourceID, count := range aggObj.Agg {
		valueInt, ok := count.(float64)
		if !ok || valueInt <= 0 {
			continue
		}
		sourceIdToUnparsedEventCount[sourceID] = valueInt
	}
	sourceIdToEventsCount := make(map[string]float64)
	for sourceID, count := range totalEventsObj.Agg {
		valueInt, ok := count.(float64)
		if !ok || valueInt <= 0 {
			continue
		}
		sourceIdToEventsCount[sourceID] = valueInt
	}

	// resolve alerts for sources which did not have unparsed events this time
	err = resolveExistingAlerts(ctx, sourceIdToUnparsedEventCount)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while resolving existing alerts", zap.Error(err))
		return err
	}

	// get all logSources for which are active alert has to be raised
	var alertToBeRaisedLogSources []source.Source
	err = config.GetDB().Model(&source.Source{}).Where("id in ?", MapKeys(sourceIdToUnparsedEventCount)).Debug().Find(&alertToBeRaisedLogSources).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting active logSources not receiving stats", zap.Error(err))
		return err
	}

	// raise alerts for sources which have unparsed events

	logging.GetLogger().Info("Raising alerts for sources which have unparsed events", zap.Any("alertToBeRaisedLogSources", MapKeys(sourceIdToUnparsedEventCount)))

	for _, ls := range alertToBeRaisedLogSources {
		unparsedCount := sourceIdToUnparsedEventCount[ls.ID.String()]
		eventsCount := sourceIdToEventsCount[ls.ID.String()]
		percentageUnparsed := (unparsedCount / eventsCount) * 100
		alertMessageStr := fmt.Sprintf("Source %s has unparsed events, accounting for %.2f%% of the total events.", ls.Name, percentageUnparsed)

		alert := alerts_common.AlertBaseObjectV2{
			EntityName:       ls.Name,
			EntityId:         ls.ID,
			EntityTenantUUId: ls.TenantID,
			AlertType:        alerts_common.AlertTypeExternalAndExternal,
			DataPlaneId:      ls.DataPlaneId,
			ErrorCode:        healthchecker.DNDW10004,
		}

		logging.GetLoggerWithContext(ctx).Info("Sending alert to control panel", zap.Any("alert", alert), zap.String("alertMessage", alertMessageStr))

		err = helper.SendAlertToControlPlane(ctx, []alerts_common.AlertBaseObjectV2{alert}, "Unparsed events detected", alertMessageStr, "UNPARSED_EVENTS_DETECTED", "DAILY_UNPARSED_EVENTS", alerts_common.CriticalAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while raising alert for unparsed events", zap.Error(err))
			continue
		}
		logging.GetLoggerWithContext(ctx).Info("Alert successfully raised for unparsed events", zap.String("sourceID", ls.ID.String()), zap.String("sourceName", ls.Name))
	}
	return err
}

func resolveExistingAlerts(ctx context.Context, sources map[string]float64) error {
	alerts, err := getExistingAlertsForUnparsedEvents(ctx, sources)
	if err != nil {
		return err
	}
	if len(alerts) == 0 {
		logging.GetLoggerWithContext(ctx).Info("no existing alerts found for unparsed events.")
		return nil
	}
	var toDismissAlerts []alerts_common.AlertBaseObjectV2
	for _, alert := range alerts {
		if _, ok := sources[alert.FunctionalityEntityId]; !ok {
			// resolve the alert as it is not present in the current list of sources having unparsed events
			toDismissAlerts = append(toDismissAlerts, alerts_common.AlertBaseObjectV2{
				EntityId:         utils.UUIDFromStringOrNil(alert.FunctionalityEntityId),
				EntityTenantUUId: utils.UUIDFromStringOrNil(alert.TenantId),
				EntityName:       alert.FunctionalityEntityName,
			})
		}
	}

	if len(toDismissAlerts) > 0 {
		err = helper.SendAlertToControlPlane(ctx, toDismissAlerts, "Unparsed Events Resolved", "All unparsed events for the specified sources have been resolved.", "UNPARSED_EVENTS_DETECTED", "DAILY_UNPARSED_EVENTS", alerts_common.CriticalAlert, alerts_common.AlertAutoResolved, true, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while dismissing alerts for unparsed events", zap.Error(err))
			return err
		}
		return nil
	}
	logging.GetLoggerWithContext(ctx).Info("dismissed alert for sources which did not had unparsed events this time.", zap.Int("dismissed_alerts_count", len(toDismissAlerts)))
	return nil
}

func getExistingAlertsForUnparsedEvents(ctx context.Context, sources map[string]float64) ([]statistics.AlertDocument, error) {
	if len(sources) == 0 {
		logging.GetLoggerWithContext(ctx).Info("No sources provided. Skipping resolution.")
		return []statistics.AlertDocument{}, nil
	}
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to OpenSearch", zap.Error(err))
		return nil, err
	}
	q := `dismissed:false AND functionalityEntityId:` + "(" + strings.Join(MapKeys(sources), " OR ") + ")" + ` AND functionalityType:UNPARSED_EVENTS`
	searchResponse, err := os.Search(ctx, client, common.AlertsIndex, q)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying openSearch for unresolved alerts", zap.Error(err))
		return nil, err
	}

	var alerts []statistics.AlertDocument
	decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
	err = decoder.Decode(searchResponse)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while decoding openSearch response", zap.Error(err))
		return nil, err
	}
	return alerts, nil
}
func GetEventDeliveryStats(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {
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

func MapKeys(inputMap map[string]float64) []string {
	keys := make([]string, 0, len(inputMap))
	for key := range inputMap {
		keys = append(keys, key)
	}
	return keys
}
