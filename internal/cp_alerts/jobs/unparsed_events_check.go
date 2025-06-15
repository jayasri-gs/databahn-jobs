package jobs

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/constants"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"strconv"
	"time"

	"github.com/mitchellh/mapstructure"
)

func SendAlertsForUnparsedEvents(ctx context.Context) error {
	db := config.GetDB()
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return err
	}
	osClient := os.GetClient()

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return err
	}
	defer func() {
		alertsManager.Close(ctx)
	}()

	for _, t := range tenants {
		tenantUuid := t.Id
		tenantId := tenantUuid.String()
		logger.GetLogger().Info("checking for unparsed events", zap.String("tenantId", tenantId))

		endTime := time.Now().UTC()
		startTime := endTime.Add(-time.Hour)
		unparsedAgg, err := getUnparsedEventsForTenant(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())), tenantId)
		if err != nil {
			logger.GetLogger().Error("error getting unparsed events", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		sourceIdToUnparsedCount := make(map[string]float64)
		for sourceId, count := range unparsedAgg.Agg {
			if v, ok := count.(float64); ok && v > 0 {
				sourceIdToUnparsedCount[sourceId] = v
			}
		}

		sourceDbPage := 0
		sourceDbPageSize := 50
		var sourcesToAlert []*model.UnparsedEventSource
		var sourcesToDismiss []*source.Source
		for {
			sources, err := readSourcesPaginated(db, t.Id, sourceDbPage, sourceDbPageSize)
			if err != nil {
				logger.GetLogger().Error("error while reading sources", zap.Error(err))
				return err
			}
			if len(sources) == 0 {
				break
			}
			for _, s := range sources {
				if _, ok := sourceIdToUnparsedCount[s.ID.String()]; ok {
					ias := model.NewUnparsedEventSource(&s)
					sourcesToAlert = append(sourcesToAlert, ias)
				} else {
					sourcesToDismiss = append(sourcesToDismiss, &s)
				}
			}
			sourceDbPage++
		}

		if len(sourcesToAlert) > 0 {
			err = sendInAppAlertsForUnparsedEvents(sourcesToAlert, alertsManager)
			if err != nil {
				logger.GetLogger().Error("error while sending unparsed events", zap.Error(err), zap.String("tenantId", tenantId))
			}
		}

		if len(sourcesToDismiss) > 0 {
			var alertsToDismiss []string
			for _, src := range sourcesToDismiss {

				q := "tenantId:" + tenantId + " AND dismissed:false AND functionalityEntityId:" + src.ID.String() + " AND functionalityType:UNPARSED_EVENTS"
				openAlerts, err := os.Search(ctx, osClient, common.AlertsIndex, q)
				if err != nil {
					logger.GetLogger().Error("error while searching for unparsed events alerts", zap.Error(err), zap.String("query", q))
					return err
				}
				var alerts []statistics.AlertDocument
				decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
				if err != nil {
					logger.GetLogger().Error("error while creating decoder for alerts", zap.Error(err))
					return err
				}
				err = decoder.Decode(openAlerts)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error while decoding openSearch response", zap.Error(err), zap.String("tenantId", tenantId))
					return err
				}
				for _, alrt := range alerts {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
			if len(alertsToDismiss) > 0 {
				err = alertsManager.AutoResolveAlerts(alertsToDismiss)
				if err != nil {
					logger.GetLogger().Error("error while dismissing unparsed events alerts", zap.Error(err))
					return err
				}
			}
		}
	}
	return nil
}

func getUnparsedEventsForTenant(ctx context.Context, startTime string, endTime string, tenantId string) (statistics.AggregateResponse, error) {

	statsAlias := os.StatisticsIndexAlias(tenantId)

	q := `tags.component_name: "parser" AND name: "total_events_delivered" AND namespace:"parsing-service-unparsed"`
	query := statistics.AddDateRange(q, startTime, endTime)
	groupBy := []string{"tags.db_event_source_id.keyword"}
	aggregations := []os.AggregationFunction{
		{Name: "sum_value", Function: "sum", Field: "counter.value"},
	}

	var allResponses []os.AggResponse
	var after map[string]any

	for {
		responses, nextAfter, err := os.CompositePaginatedAggregate(ctx, os.GetClient(), 200, statsAlias, query, groupBy, aggregations, after)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err))
			return statistics.AggregateResponse{}, err
		}

		allResponses = append(allResponses, responses...)

		if nextAfter == nil {
			break
		}
		after = nextAfter
	}

	logger.GetLoggerWithContext(ctx).Info("Number of responses received", zap.Int("count", len(allResponses)))

	aggMap := make(map[string]any)
	for _, resp := range allResponses {
		if sumValue, ok := resp.Values["sum_value"]; ok {
			aggMap[resp.Key["tags.db_event_source_id.keyword"].(string)] = sumValue
		}
	}

	return statistics.AggregateResponse{Agg: aggMap}, nil
}

func sendInAppAlertsForUnparsedEvents(sourcesToAlert []*model.UnparsedEventSource, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(sourcesToAlert))
	for i, srcAlert := range sourcesToAlert {
		newAlert, err := buildUnparsedEventAlert(*srcAlert)
		if err != nil {
			logger.GetLogger().Error("error while building alert", zap.Error(err))
			return err
		}
		alertsToSave[i] = newAlert
	}
	err := alertsManager.SendAlerts(alertsToSave)
	if err != nil {
		logger.GetLogger().Error("error while sending inactive source alert", zap.Error(err))
		return err
	}
	return nil
}

func buildUnparsedEventAlert(ias model.UnparsedEventSource) (*alerts_async.Alert, error) {
	details := constants.UnparsedEventCheckerFunctionalityType
	functionality := alerts_async.LogSource
	return alerts_async.NewAlert(functionality,
		alerts_async.WithEntity(ias),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.UnparsedChecker),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DBPW10001, ""),
	)
}
