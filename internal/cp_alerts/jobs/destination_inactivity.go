package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/constants"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strings"
	"time"
)

func AlertForNoEventsToDestination(ctx context.Context) error {
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
		tenantId := t.Id.String()

		logger.GetLoggerWithContext(ctx).Info("checking for inactive destinations", zap.String("tenant_id", tenantId))

		destinationsIdToLastEventTime, err := getDestinationIdToLastEventTime(ctx, osClient, tenantId)
		if err != nil {
			return err
		}
		destinationsToAlert, activeDestinations, err := findInactiveAndActiveDestinations(db, tenantUuid, destinationsIdToLastEventTime)

		if err != nil {
			return err
		}

		if len(destinationsToAlert) == 0 {
			logger.GetLoggerWithContext(ctx).Info("no  destinations to alert for tenant", zap.String("tenant_id", tenantId))
		} else {
			// raise in app alerts
			err = sendInAppAlertsForDestination(destinationsToAlert, alertsManager)
			if err != nil {
				return err
			}
		}

		activeDestinationsById := make(map[string]*destination.Destination)
		for _, d := range activeDestinations {
			activeDestinationsById[d.ID.String()] = d
		}
		activeDestinationsPartitions := util.PartitionSlice(activeDestinations, 10)
		var alertsToDismiss []string
		for _, activeDestinationsPartition := range activeDestinationsPartitions {
			destinationIds := make([]string, len(activeDestinationsPartition))
			for i, d := range activeDestinationsPartition {
				destinationIds[i] = d.ID.String()
			}
			q := "tenantId:" + tenantId + " AND dismissed:false AND functionalityEntityId:" + "(" + strings.Join(destinationIds, " OR ") + ")" + ` AND functionalityType:(` + constants.DeliveryCheckerFunctionalityType + " OR " + alerts_async.IngestionChecker.String() + ")"
			openAlerts, err := os.Search(ctx, osClient, common.AlertsIndex, q)
			if err != nil {
				logger.GetLogger().Error("error while searching for alerts", zap.Error(err), zap.String("query", q))
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
			if len(alerts) == 0 {
				logger.GetLoggerWithContext(ctx).Info("no open alerts for active destinations", zap.String("tenant_id", tenantId), zap.Strings("destination_ids", destinationIds))
				continue
			}
			for _, alrt := range alerts {
				if _, ok := activeDestinationsById[alrt.FunctionalityEntityId]; ok {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
		}
		if len(alertsToDismiss) > 0 {
			err = alertsManager.AutoResolveAlerts(alertsToDismiss)
			logger.GetLogger().Info("alert dismissed for tenant", zap.String("tenant_id", tenantId), zap.Any("destination_id", alertsToDismiss))
			if err != nil {
				logger.GetLogger().Error("error while dismissing alerts", zap.Error(err))
			}
		}
	}
	return nil
}

func sendInAppAlertsForDestination(destinationsToAlert []*model.InactiveDestination, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(destinationsToAlert))
	for i, destAlert := range destinationsToAlert {
		newAlert, err := buildDestAlert(*destAlert)
		if err != nil {
			logger.GetLogger().Error("error while building alert", zap.Error(err), zap.String("destination_id", destAlert.Destination.ID.String()))
			return err
		}
		alertsToSave[i] = newAlert

	}
	err := alertsManager.SendAlerts(alertsToSave)
	if err != nil {
		logger.GetLogger().Error("error while sending inactive destination alerts", zap.Error(err))
		return err
	}
	return nil
}

func findInactiveAndActiveDestinations(db *gorm.DB, tenantUuid uuid.UUID, destinationIdToLastEventTime map[string]time.Time) ([]*model.InactiveDestination, []*destination.Destination, error) {
	var destinationsToAlert []*model.InactiveDestination
	var activeDestinations []*destination.Destination

	destinationDbPage := 0
	destinationDbPageSize := 50

	for {
		destinations, err := readDestinationsPaginated(db, tenantUuid, destinationDbPage, destinationDbPageSize)
		if err != nil {
			logger.GetLogger().Error("error while reading destinations", zap.Error(err))
			return nil, nil, err
		}

		logger.GetLogger().Info("found destinations", zap.Any("Destinations", destinations))

		if len(destinations) == 0 {
			break
		}
		for _, d := range destinations {
			destinationId := d.ID.String()

			logger.GetLogger().Info("Checking for destination", zap.String("destination_id", destinationId))

			lastEventTime, ok := destinationIdToLastEventTime[destinationId]

			logger.GetLogger().Info("Last event time for destination", zap.String("destination_id", destinationId), zap.String("last_event_time", lastEventTime.String()))

			if !ok {
				logger.GetLogger().Warn("no last event time found for destination", zap.String("destination_id", destinationId))
				continue
			}
			now := time.Now().UTC()
			if now.Sub(lastEventTime) > defaultAlertDuration30Min {
				if lastEventTime.Before(now.Add(-defaultRequiredEventsInLastSevenDays)) {
					logger.GetLogger().Info("destination received data older than 7 days, not eligible for alert", zap.String("destinationId", destinationId), zap.String("tenantId", tenantUuid.String()), zap.Duration("alertDuration", defaultAlertDuration30Min), zap.Time("lastEventTime", lastEventTime))
					continue
				}
				iad := model.NewInactiveDestination(&d, lastEventTime)
				destinationsToAlert = append(destinationsToAlert, iad)
				logger.GetLogger().Info("Alerting destinations", zap.Any("alert_destinations", destinationsToAlert))
			} else {
				logger.GetLogger().Info("destination received data within alert duration , not eligible for alert", zap.String("destination_id", destinationId), zap.Duration("duration ", defaultAlertDuration30Min), zap.Time("last_event_time", lastEventTime))
				activeDestinations = append(activeDestinations, &d)
			}
		}
		destinationDbPage += 1
	}
	return destinationsToAlert, activeDestinations, nil
}

func getDestinationIdToLastEventTime(ctx context.Context, osClient *opensearch.Client, tenantId string) (map[string]time.Time, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)

	aggFunc := os.AggregationFunction{
		Function: "max",
		Field:    "tags.db_ts_win",
		Name:     "last_event_time",
	}

	var destinationIdToLastEventTime = make(map[string]time.Time)

	var after map[string]any = nil

	q := `tags.component_name: "dispenser" AND name: "total_events_delivered"`

	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, osClient, 100, statsAlias, q, []string{"tags.destination_id.keyword"}, []os.AggregationFunction{aggFunc}, after)
		if err != nil {
			return nil, err
		}
		if len(responses) == 0 {
			break
		}
		for _, response := range responses {
			destinationId := response.Key["tags.destination_id.keyword"].(string)
			lastEventMillis := int64(response.Values["last_event_time"].(float64))
			lastEventTime := time.UnixMilli(lastEventMillis).UTC()
			destinationIdToLastEventTime[destinationId] = lastEventTime
		}

		after = newAfter
	}
	return destinationIdToLastEventTime, nil

}
func buildDestAlert(iad model.InactiveDestination) (*alerts_async.Alert, error) {
	now := time.Now().UTC()
	actualDifference := now.Sub(iad.LastEventTime)
	title := fmt.Sprintf(constants.DeliveryCheckerFunctionalityTitle, util.HumanReadableDuration(actualDifference))
	message := fmt.Sprintf(constants.DeliveryCheckerFunctionalityMessage, util.HumanReadableDuration(defaultAlertDuration30Min),
		util.HumanReadableTimeWithZone(now), util.HumanReadableTimeWithZone(iad.LastEventTime))
	functionality := alerts_async.Dispenser

	return alerts_async.NewAlert(functionality,
		alerts_async.WithEntity(iad),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.DeliveryChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10002, ""),
	)
}

func readDestinationsPaginated(db *gorm.DB, tenantId uuid.UUID, page, pageSize int) ([]destination.Destination, error) {

	var destinations []destination.Destination
	offset := page * pageSize

	result := db.Where("tenant_id = ? AND status = 'ACTIVE' ", tenantId).
		Limit(pageSize).
		Offset(offset).
		Find(&destinations)

	return destinations, result.Error

}
