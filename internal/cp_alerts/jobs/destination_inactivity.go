package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"

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
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func AlertForNoEventsToDestination(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		errorMsg := fmt.Sprintf("error while getting tenants: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return common.NewJobResult(jobErrors, false)
	}

	osClient := os.GetClient()

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error while creating alerts manager: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return common.NewJobResult(jobErrors, false)
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
			errorMsg := fmt.Sprintf("error getting destination event times for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error getting destination event times", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}
		destinationsToAlert, activeDestinations, err := findInactiveAndActiveDestinations(db, tenantUuid, destinationsIdToLastEventTime)

		if err != nil {
			errorMsg := fmt.Sprintf("error finding inactive destinations for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error finding inactive destinations", zap.Error(err), zap.String("tenantId", tenantId))
			continue
		}

		if len(destinationsToAlert) == 0 {
			logger.GetLoggerWithContext(ctx).Info("no  destinations to alert for tenant", zap.String("tenant_id", tenantId))
		} else {
			// raise in app alerts
			err = sendInAppAlertsForDestination(destinationsToAlert, alertsManager)
			if err != nil {
				errorMsg := fmt.Sprintf("error sending in-app alerts for destinations for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error sending in-app alerts for destinations", zap.Error(err), zap.String("tenantId", tenantId))
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
			q := "tenantId:" + tenantId + " AND dismissed:false AND functionalityEntityId:" + "(" + strings.Join(destinationIds, " OR ") + ")" + ` AND functionalityType:(` + constants.DeliveryCheckerFunctionalityType + " OR " + alerts_async.DeliveryChecker.String() + ")"
			openAlerts, _, err := os.Search(ctx, osClient, common.AlertsIndex, q)
			if err != nil {
				errorMsg := fmt.Sprintf("error while searching for alerts for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error while searching for alerts", zap.Error(err), zap.String("query", q), zap.String("tenantId", tenantId))
				continue
			}
			alerts, err := statistics.ParseAlertDocuments(openAlerts)
			if err != nil {
				errorMsg := fmt.Sprintf("error while decoding openSearch response for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLoggerWithContext(ctx).Error("error while decoding openSearch response", zap.Error(err), zap.String("tenantId", tenantId))
				continue
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
			logger.GetLogger().Info("alert dismissed for tenant", zap.String("tenant_id", tenantId), zap.Any("alertsToDismiss", alertsToDismiss))
			if err != nil {
				errorMsg := fmt.Sprintf("error while dismissing alerts for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error while dismissing alerts", zap.Error(err), zap.String("tenantId", tenantId))
			}
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("successfully completed destination inactivity check")
		return common.NewJobResult([]common.JobError{}, true)
	} else {
		logger.GetLogger().Info("destination inactivity check completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResult(jobErrors, false)
	}
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
		var destinationIds []uuid.UUID
		for _, s := range destinations {
			destinationIds = append(destinationIds, s.ID)
		}

		alertConfigs, err := entities.ReadEntityConfigs(db, entities.DestinationEntityType, "DESTINATION_INACTIVITY", tenantUuid, destinationIds)
		if err != nil {
			logger.GetLogger().Error("error while reading destination entity configs", zap.Error(err))
			return nil, nil, err
		}
		alertConfigsByDestinationId := make(map[uuid.UUID]entities.EntityAlertsConfig)
		for _, alertConfig := range alertConfigs {
			alertConfigsByDestinationId[alertConfig.EntityID] = alertConfig
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
			alertConfig, ok := alertConfigsByDestinationId[d.ID]
			var alertDuration time.Duration
			if ok {
				configuredDuration, errr, skip := getAlertDurationForDestination(alertConfig, destinationId, tenantUuid.String())
				if errr != nil {
					logger.GetLogger().Error("error while getting alert duration, ignoring", zap.Error(errr), zap.String("destinationId", destinationId), zap.String("tenantId", tenantUuid.String()))
					continue
				}
				if skip {
					logger.GetLogger().Warn("alert config is disabled, skipping", zap.String("destinationId", destinationId), zap.String("tenantId", tenantUuid.String()))
					continue
				}
				alertDuration = configuredDuration
			} else {
				logger.GetLogger().Warn("no alert config found for source, defaulting", zap.String("destinationId", destinationId), zap.String("tenantId", tenantUuid.String()))
				alertDuration = defaultAlertDuration30Min
			}
			now := time.Now().UTC()
			if now.Sub(lastEventTime) > alertDuration {
				if lastEventTime.Before(now.Add(-defaultRequiredEventsInLastSevenDays)) {
					logger.GetLogger().Info("destination received data older than 7 days, not eligible for alert", zap.String("destinationId", destinationId), zap.String("tenantId", tenantUuid.String()), zap.Duration("alertDuration", defaultAlertDuration30Min), zap.Time("lastEventTime", lastEventTime))
					continue
				}
				iad := model.NewInactiveDestination(&d, lastEventTime, alertDuration, now)
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

func getAlertDurationForDestination(alertConfig entities.EntityAlertsConfig, destinationId, tenantId string) (time.Duration, error, bool) {
	dbConfig := alertConfig.Config
	if dbConfig == nil {
		logger.GetLogger().Warn("no alert config found for destination, defaulting", zap.String("destinationId", destinationId), zap.String("tenantId", tenantId))
		return defaultAlertDuration30Min, nil, false
	}
	if !dbConfig.Enabled {
		logger.GetLogger().Warn("alert config is disabled for destination, not notifying", zap.String("destinationId", destinationId), zap.String("tenantId", tenantId))
		return defaultAlertDuration30Min, nil, true
	}
	inactivityAlertConfig := dbConfig.DestinationInactivityAlertConfig
	if inactivityAlertConfig == nil {
		logger.GetLogger().Warn("no alert config DestinationInactivityAlertConfig found for destination, defaulting", zap.String("destinationId", destinationId), zap.String("tenantId", tenantId))
		return defaultAlertDuration30Min, nil, false
	}
	duration := inactivityAlertConfig.InactivityDuration
	if duration == nil {
		logger.GetLogger().Warn("no alert config DestinationInactivityAlertConfig InactivityDuration found for destination, defaulting", zap.String("destinationId", destinationId), zap.String("tenantId", tenantId))
		return defaultAlertDuration30Min, nil, false
	}
	dur, err := duration.GetDuration()
	return dur, err, false
}

func getDestinationIdToLastEventTime(ctx context.Context, osClient *opensearch.Client, tenantId string) (map[string]time.Time, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)

	aggFunc := os.AggregationFunction{
		Function: "max",
		Field:    "timestamp",
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
	message := fmt.Sprintf(constants.DeliveryCheckerFunctionalityMessage, util.HumanReadableDuration(iad.AlertDuration),
		util.HumanReadableTimeWithZone(iad.CheckedAt), util.HumanReadableTimeWithZone(iad.LastEventTime))
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
