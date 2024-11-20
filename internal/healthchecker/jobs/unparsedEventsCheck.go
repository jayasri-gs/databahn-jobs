package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
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

	logging.GetLoggerWithContext(ctx).Info("Handling hourly alerts for unparsed events")

	endTime := time.Now()
	startTime := endTime.Add(-time.Hour)

	aggObj, err := GetUnparsedEvents(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting unparsed events stats", zap.Error(err))
		return err
	}

	unparsedEventCounts := make(map[string]float64)
	for sourceID, count := range aggObj.Agg {
		valueInt, ok := count.(float64)
		if !ok || valueInt <= 0 {
			continue
		}
		unparsedEventCounts[sourceID] = valueInt
	}

	if len(unparsedEventCounts) == 0 {
		logging.GetLoggerWithContext(ctx).Info("No unparsed events detected. Resolving existing alerts.")
		err := resolveExistingAlerts(ctx, MapKeys(unparsedEventCounts))
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while resolving existing alerts", zap.Error(err))
			return err
		}
		return nil
	}

	var alertSources []source.Source
	err = config.GetDB().Model(&source.Source{}).
		Where("status not in ? AND id in ?", []string{"inactive", "deleted"}, MapKeys(unparsedEventCounts)).
		Debug().Find(&alertSources).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching active sources with unparsed events", zap.Error(err))
		return err
	}

	var toRaiseAlerts []alerts_common.AlertEntityObject
	var toRaiseAlertsDetails []alerts_common.Alert
	for _, src := range alertSources {
		count := unparsedEventCounts[src.ID.String()]
		toRaiseAlerts = append(toRaiseAlerts, alerts_common.AlertEntityObject{
			EntityName:       src.Name,
			EntityId:         utils.UUIDFromStringOrNil(src.ID.String()),
			EntityTenantUUId: src.TenantID,
		})

		toRaiseAlertsDetails = append(toRaiseAlertsDetails, alerts_common.Alert{
			Title:                   "Unparsed Events Detected",
			Message:                 fmt.Sprintf("Detected %v unparsed events for source '%s' in the last hour.", count, src.Name),
			CreatedAt:               time.Now(),
			UpdatedAt:               time.Now(),
			FirstObservedAt:         time.Now(),
			LastObservedAt:          time.Now(),
			TenantUUID:              src.TenantID,
			FunctionalityType:       "UNPARSED_EVENTS",
			Functionality:           "DAILY_UNPARSED_EVENTS",
			FunctionalityEntityId:   src.ID.String(),
			FunctionalityEntityName: src.Name,
			Dismissed:               false,
			Criticality:             alerts_common.WarningAlert,
			Status:                  alerts_common.AlertOpen,
			UpdatedBy:               "system",
		})
	}

	if len(toRaiseAlerts) > 0 {
		err := helper.SendAlertToControlPlane(
			ctx,
			toRaiseAlerts,
			"Unparsed Events Detected",
			"Unparsed events were detected for the specified sources in the last hour.",
			"UNPARSED_EVENTS",
			"DAILY_UNPARSED_EVENTS",
			alerts_common.WarningAlert,
			alerts_common.AlertOpen,
			false,
			"system",
		)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while raising alert for unparsed events", zap.Error(err))
			return err
		}

		logging.GetLoggerWithContext(ctx).Info("Alerts successfully raised for unparsed events", zap.Int("raised_alerts_count", len(toRaiseAlerts)))
	}

	return nil
}

func resolveExistingAlerts(ctx context.Context, sources []string) error {
	logging.GetLoggerWithContext(ctx).Info("Resolving existing alerts for unparsed events in OpenSearch.")

	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to OpenSearch", zap.Error(err))
		return err
	}

	q := `dismissed:false AND functionalityEntityId:` + "(" + strings.Join(sources, " OR ") + ")" + ` AND functionalityType:UNPARSED_EVENTS`

	searchResponse, err := os.Search(ctx, client, common.AlertsIndex, q)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying OpenSearch for unresolved alerts", zap.Error(err), zap.String("query", q), zap.String("index", common.AlertsIndex))
		return err
	}

	var alerts []statistics.AlertDocument
	decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
	err = decoder.Decode(searchResponse)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while decoding OpenSearch response", zap.Error(err))
		return err
	}

	if len(alerts) == 0 {
		logging.GetLoggerWithContext(ctx).Info("No unresolved alerts found in OpenSearch.")
		return nil
	}

	var toDismissAlerts []alerts_common.AlertEntityObject
	for _, alert := range alerts {
		var temp alerts_common.AlertEntityObject
		temp.EntityName = alert.FunctionalityEntityName
		temp.EntityId = utils.UUIDFromStringOrNil(alert.FunctionalityEntityId)
		temp.EntityTenantUUId = utils.UUIDFromStringOrNil(alert.TenantId)
		toDismissAlerts = append(toDismissAlerts, temp)
	}

	if len(toDismissAlerts) > 0 {
		err = helper.SendAlertToControlPlane(
			ctx,
			toDismissAlerts,
			"Unparsed Events Resolved",
			"All unparsed events for the specified sources have been resolved.",
			"UNPARSED_EVENTS",
			"DAILY_UNPARSED_EVENTS",
			alerts_common.WarningAlert,
			alerts_common.AlertAutoResolved,
			true,
			"system",
		)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while dismissing alerts for unparsed events", zap.Error(err))
			return err
		}
	}

	logging.GetLoggerWithContext(ctx).Info("Successfully resolved alerts for unparsed events.", zap.Int("resolved_alerts_count", len(toDismissAlerts)))
	return nil
}

func MapKeys(inputMap map[string]float64) []string {
	keys := make([]string, 0, len(inputMap))
	for key := range inputMap {
		keys = append(keys, key)
	}
	return keys
}
