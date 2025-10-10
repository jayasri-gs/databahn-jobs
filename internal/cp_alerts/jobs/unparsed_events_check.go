package jobs

import (
	"context"
	"fmt"
	"strconv"
	"time"

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
)

const (
	MinUnparsedEventsPercentage = 1.0
	UnparsedEventsCheckDuration = 24
)

func SendAlertsForUnparsedEvents(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	db := config.GetDB()
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		errorMsg := fmt.Sprintf("error while getting tenants: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}
	osClient := os.GetClient()

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error while creating alerts manager: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}
	defer func() {
		alertsManager.Close(ctx)
	}()

	for _, t := range tenants {

		tenantUuid := t.Id
		tenantId := tenantUuid.String()
		logger.GetLogger().Info("checking for unparsed events", zap.String("tenantId", tenantId))

		endTime := time.Now().UTC()
		startTime := endTime.Add(-UnparsedEventsCheckDuration * time.Hour)
		statsAlias := os.StatisticsIndexAlias(tenantId)
		unparsedAgg, err := GetUnparsedEventsForTenant(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())), statsAlias)
		if err != nil {
			errorMsg := fmt.Sprintf("error getting unparsed events for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error getting unparsed events", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		totalEventsAgg, err := GetTotalEventsForTenant(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())), statsAlias)
		if err != nil {
			errorMsg := fmt.Sprintf("error getting total events for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error getting total events", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		sourceIdToUnparsedCount := make(map[string]float64)
		sourceIdToTotalCount := make(map[string]float64)
		sourceIdToUnparsedPercentage := make(map[string]float64)

		for sourceId, count := range totalEventsAgg.Agg {
			if v, ok := count.(float64); ok && v > 0 {
				sourceIdToTotalCount[sourceId] = v
			}
		}

		for sourceId, unparsedCount := range unparsedAgg.Agg {
			if v, ok := unparsedCount.(float64); ok && v > 0 {
				sourceIdToUnparsedCount[sourceId] = v

				if totalCount, hasTotal := sourceIdToTotalCount[sourceId]; hasTotal && totalCount > 0 {
					percentage := (v / totalCount) * 100
					sourceIdToUnparsedPercentage[sourceId] = percentage

					logger.GetLogger().Info("calculated unparsed percentage",
						zap.String("sourceId", sourceId),
						zap.Float64("unparsedCount", v),
						zap.Float64("totalCount", totalCount),
						zap.Float64("percentage", percentage))
				}
			}
		}

		sourceDbPage := 0
		sourceDbPageSize := 50
		var sourcesToAlert []*model.UnparsedEventSource
		var sourcesToDismiss []*source.Source
		for {
			sources, err := readSourcesPaginated(db, t.Id, sourceDbPage, sourceDbPageSize)
			if err != nil {
				errorMsg := fmt.Sprintf("error while reading sources for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error while reading sources", zap.Error(err), zap.String("tenantId", tenantId))
				return common.NewJobResultFromErrors(jobErrors)
			}
			if len(sources) == 0 {
				break
			}
			for _, s := range sources {
				sourceId := s.ID.String()
				if percentage, hasPercentage := sourceIdToUnparsedPercentage[sourceId]; hasPercentage {
					if percentage >= MinUnparsedEventsPercentage {
						unparsedCount := int(sourceIdToUnparsedCount[sourceId])
						ias := model.NewUnparsedEventSource(&s, unparsedCount, percentage)
						sourcesToAlert = append(sourcesToAlert, ias)
						logger.GetLogger().Info("source meets alert threshold",
							zap.String("sourceId", sourceId),
							zap.Float64("percentage", percentage),
							zap.Int("unparsedCount", unparsedCount))
					} else {
						logger.GetLogger().Info("source below alert threshold",
							zap.String("sourceId", sourceId),
							zap.Float64("percentage", percentage))
					}
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
				openAlerts, _, err := os.Search(ctx, osClient, common.AlertsIndex, q)
				if err != nil {
					errorMsg := fmt.Sprintf("error while searching for unparsed events alerts for tenant %s: %v", tenantId, err)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLogger().Error("error while searching for unparsed events alerts", zap.Error(err), zap.String("query", q), zap.String("tenantId", tenantId))
					return common.NewJobResultFromErrors(jobErrors)
				}
				alerts, err := statistics.ParseAlertDocuments(openAlerts)
				if err != nil {
					errorMsg := fmt.Sprintf("error while decoding openSearch response for tenant %s: %v", tenantId, err)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLoggerWithContext(ctx).Error("error while decoding openSearch response", zap.Error(err), zap.String("tenantId", tenantId))
					return common.NewJobResultFromErrors(jobErrors)
				}
				for _, alrt := range alerts {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
			if len(alertsToDismiss) > 0 {
				err = alertsManager.AutoResolveAlerts(alertsToDismiss)
				if err != nil {
					errorMsg := fmt.Sprintf("error while dismissing unparsed events alerts for tenant %s: %v", tenantId, err)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLogger().Error("error while dismissing unparsed events alerts", zap.Error(err), zap.String("tenantId", tenantId))
				}
			}
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("successfully completed unparsed events check")
		return common.NewJobResultSuccess()
	} else {
		logger.GetLogger().Info("unparsed events check completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}

func GetUnparsedEventsForTenant(ctx context.Context, startTime, endTime, statsAlias string) (statistics.AggregateResponse, error) {

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

func GetTotalEventsForTenant(ctx context.Context, startTime, endTime, statsAlias string) (statistics.AggregateResponse, error) {

	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
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

	logger.GetLoggerWithContext(ctx).Info("Number of total events responses received", zap.Int("count", len(allResponses)))

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
	title := fmt.Sprintf(constants.UnparsedEventCheckerFunctionalityTitle, ias.GetEntityName(), ias.GetUnparsedCount(), ias.GetPercentage(), UnparsedEventsCheckDuration)
	message := fmt.Sprintf(constants.UnparsedEventCheckerFunctionalityMessage, ias.GetEntityName(), ias.GetUnparsedCount(), ias.GetPercentage())

	functionality := alerts_async.LogSource
	if ias.Source.Scope == "CLOUD" {
		functionality = alerts_async.CloudLogSource
	}

	return alerts_async.NewAlert(functionality,
		alerts_async.WithEntity(ias),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.UnparsedChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DBPW10001, ""),
		alerts_async.WithAction("If the log source is custom, please check the parsers, otherwise please contact Databahn Team. You may also want to check the type of data being received for the log source."),
	)
}
