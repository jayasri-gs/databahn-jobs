package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
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

const (
	DefaultLastSevenDays = 7 * 24 * time.Hour
)

func SendAlertsForInactivity(ctx context.Context) error {
	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Info("Failed to get tenants")
		return err
	}

	alertConfigMap, err := helper.EntityAlertConfigMapToTenantId(db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Info("Failed to load alert config")
		return err
	}

	for _, t := range tenants {

		logging.GetLoggerWithContext(ctx).Info("processing alerts for tenant id ", zap.Any("tenantId", t.Id))

		tenantMap, ok := alertConfigMap[t.Id.String()]
		if !ok {
			continue
		}

		for interval, entityAlertsConfigs := range tenantMap {

			now := time.Now().UTC()
			var sourceIdsToAlert []string

			var activeSources []string
			lastCheckTimeMapToSourceIds := make(map[string]time.Time)
			for _, c := range entityAlertsConfigs {

				lastEventTime := c.LastCheckedTime

				lastCheckTimeMapToSourceIds[c.EntityID.String()] = lastEventTime

				if lastEventTime.Before(now.Add(-DefaultLastSevenDays)) {
					logging.GetLoggerWithContext(ctx).Info("Skipping source due to old last event time",
						zap.String("entityId", c.ID.String()),
						zap.Time("lastEventTime", lastEventTime))

					continue
				}

				if now.Sub(lastEventTime) > time.Duration(interval)*time.Minute {
					// Source is inactive, add to list for alerting
					sourceIdsToAlert = append(sourceIdsToAlert, c.EntityID.String())

				} else {
					// Source is active, add to list for potential alert dismissal
					activeSources = append(activeSources, c.EntityID.String())

				}

			}

			if len(sourceIdsToAlert) == 0 && len(activeSources) == 0 {
				logging.GetLoggerWithContext(ctx).Info("No sources to alert or dismiss for this interval.", zap.Any("interval", interval))
				continue
			}

			var validatedInactiveSources []source.Source
			statusCheck := []string{healthchecker.StatusCreated, healthchecker.StatusInactive, healthchecker.StatusDeleted, healthchecker.StatusDisabled}

			if len(sourceIdsToAlert) > 0 {
				err = db.Model(&source.Source{}).
					Where("status not in ? AND id in ?", statusCheck, sourceIdsToAlert).
					Find(&validatedInactiveSources).Error
				if err != nil {
					logging.GetLoggerWithContext(ctx).Error("Failed to get active sources from db for alerting",
						zap.Error(err), zap.Strings("sourceIds", sourceIdsToAlert))
					continue
				}
			}

			var alertsToSend []helper.AlertBaseObjectV2
			for _, validSource := range validatedInactiveSources {

				alertsToSend = append(alertsToSend, helper.AlertBaseObjectV2{
					EntityName:       validSource.Name,
					EntityId:         validSource.ID,
					EntityTenantUUId: validSource.TenantID,
					DataPlaneId:      validSource.DataPlaneId,
					AlertType:        alerts_common.AlertTypeExternalAndExternal,
					ErrorCode:        healthchecker.DNDW10001,
					Description: fmt.Sprintf(
						"No new events received in %s",
						humanizeDuration(now.Sub(lastCheckTimeMapToSourceIds[validSource.ID.String()])),
					)})
			}

			if len(activeSources) > 0 {

				alerts, err := PaginatedOpenSearchCallToGetAllExistingAlerts(ctx, activeSources)
				if err != nil {
					logging.GetLoggerWithContext(ctx).Error("error getting existing alerts for active sources", zap.Error(err))
					return err
				}

				var toDismiss []helper.AlertBaseObjectV2
				for _, alert := range alerts {
					var temp helper.AlertBaseObjectV2
					temp.EntityName = alert.FunctionalityEntityName
					temp.EntityId = utils.UUIDFromStringOrNil(alert.FunctionalityEntityId)
					temp.EntityTenantUUId = utils.UUIDFromStringOrNil(alert.TenantId)
					temp.AlertType = alerts_common.AlertTypeExternalAndExternal
					temp.ErrorCode = healthchecker.DNDW10001
					toDismiss = append(toDismiss, temp)
				}

				logging.GetLoggerWithContext(ctx).Info("Dismissing Alerts for tenant Id", zap.Any("tenantId", t.Id))
				logging.GetLoggerWithContext(ctx).Info("dismissing alerts for sources ", zap.Any("sourceToDismiss", toDismiss))

				err = helper.SendAlertToControlPlaneForLogSource(ctx, toDismiss,
					fmt.Sprintf("No new events received in the last %d minutes", interval),
					alerts_common.LogSourceStatsNotReceived,
					alerts_common.LogSourceFunctionality,
					alerts_common.SevereAlert,
					alerts_common.AlertAutoResolved,
					true,
					"system")
				if err != nil {
					logging.GetLoggerWithContext(ctx).Error("error while dismissing alerts for logSource activity check", zap.Error(err))
					return err
				}

			}

			if len(alertsToSend) > 0 {
				logging.GetLoggerWithContext(ctx).Info("Sending new alerts for tenant's inactive sources",
					zap.Any("sourcesToAlert", alertsToSend)) // Renamed from sourceToDismiss

				err = helper.SendAlertToControlPlaneForLogSource(ctx, alertsToSend,
					fmt.Sprintf("No new events received in the last %d minutes", interval),
					alerts_common.LogSourceStatsNotReceived,
					alerts_common.LogSourceFunctionality,
					alerts_common.SevereAlert,
					alerts_common.AlertOpen,
					false,
					"system",
				)
				if err != nil {
					logging.GetLoggerWithContext(ctx).Error("Error while raising alert for log source activity check",
						zap.Error(err), zap.Any("sourcesToAlert", alertsToSend))
				}
			}

		}

	}

	return nil

}

func PaginatedOpenSearchCallToGetAllExistingAlerts(ctx context.Context, logsources []string) ([]statistics.AlertDocument, error) {

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
	return allAlerts, nil

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

	// getting logSources which are not active but did not report stats in last 15 minutes
	var alertToBeRaisedDispenser []destination.Destination
	checkStatus := []string{healthchecker.StatusDisabled, healthchecker.StatusDeleted, healthchecker.StatusCreated, healthchecker.StatusInactive}
	err = config.GetDB().Model(&destination.Destination{}).Where("status not in ? AND id in ?", checkStatus, destinationIdsToAlert).Debug().Find(&alertToBeRaisedDispenser).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting active logSources not receiving stats", zap.Error(err))
		return err
	}

	//creating alertEntityArray for all logSources for which alert needs to be raised
	var logsourcesEntityArray []helper.AlertBaseObjectV2
	var silentLogsources []string
	for _, ls := range alertToBeRaisedDispenser {
		var temp helper.AlertBaseObjectV2
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
	logging.GetLogger().Info("notification stats as follows", zap.Any("no_of_inactive_sources", len(destinationStatsReceived)), zap.Any("ids", destinationIdsToAlert))
	return nil
}

func getAggStatsForDestinationPaginated(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {
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

	return statistics.AggregateResponse{Agg: aggMap}, nil
}

func humanizeDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d seconds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%d minutes", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		return fmt.Sprintf("%d hours, %d minutes", h, m)
	}
	days := int(d.Hours()) / 24
	h := int(d.Hours()) % 24
	return fmt.Sprintf("%d days, %d hours", days, h)
}
