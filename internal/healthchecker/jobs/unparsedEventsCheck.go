package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
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
		logging.GetLoggerWithContext(ctx).Info("No unparsed events detected. No alerts raised.")
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

	var alerts []alerts_common.Alert
	for _, src := range alertSources {
		count := unparsedEventCounts[src.ID.String()]
		alert := alerts_common.Alert{
			Title:                   "Unparsed Events Detected",
			Message:                 fmt.Sprintf("Detected %v unparsed events for source '%s' in the last hour.", count, src.Name),
			CreatedAt:               time.Now(),
			UpdatedAt:               time.Now(),
			FirstObservedAt:         time.Now(),
			LastObservedAt:          time.Now(),
			TenantUUID:              src.TenantID,
			FunctionalityType:       "TBD",             //alerts_common.EventSourceFunctionality // Set appropriate functionality type
			Functionality:           "UNPARSED_EVENTS", // Set specific functionality
			FunctionalityEntityId:   src.ID.String(),
			FunctionalityEntityName: src.Name,
			Dismissed:               false,
			Criticality:             alerts_common.WarningAlert,
			Status:                  alerts_common.AlertOpen,
			UpdatedBy:               "system",
		}
		alerts = append(alerts, alert)
	}

	// Raise alerts
	for _, alert := range alerts {
		err := helper.SendAlertToControlPlane(
			ctx,
			[]alerts_common.AlertEntityObject{
				{
					EntityName:       alert.FunctionalityEntityName,
					EntityId:         uuid.MustParse(alert.FunctionalityEntityId),
					EntityTenantUUId: alert.TenantUUID,
				},
			},
			alert.Title,
			alert.Message,
			"UNPARSED_EVENTS_DETECTED",
			alert.Functionality,
			alert.Criticality,
			alert.Status,
			false,
			"system",
		)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while raising alert for unparsed events", zap.Error(err))
			return err
		}
	}

	logging.GetLogger().Info("Alert raised for unparsed events", zap.Int("alerted_sources_count", len(alertSources)))
	return nil
}
func MapKeys(inputMap map[string]float64) []string {
	keys := make([]string, 0, len(inputMap))
	for key := range inputMap {
		keys = append(keys, key)
	}
	return keys
}
