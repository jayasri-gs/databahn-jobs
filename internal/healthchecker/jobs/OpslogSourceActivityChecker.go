package jobs

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"time"
)

func UpdateLastEventTime(ctx context.Context) error {

	osClient := os.GetClient()

	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error getting tenants", zap.Error(err))
		return err
	}
	fullConfigMap, err := helper.LoadEntityAlertConfigMapToTenantId(db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error creating EntityAlertsConfig map", zap.Error(err))
		return err
	}

	logging.GetLoggerWithContext(ctx).Info("configMap", zap.Any("configMap", fullConfigMap))

	for _, tenant := range tenants {
		tenantId := tenant.Id.String()

		logger := logging.GetLoggerWithContext(ctx).With(zap.String("tenantId", tenantId))
		logger.Info("Processing tenant for last event time updates")

		tenantLogSourcesByInterval, ok := fullConfigMap[tenantId]
		if !ok {
			logger.Info("No log sources found for tenant", zap.String("tenantId", tenantId))
			continue
		}

		for interval, logSourceIds := range tenantLogSourcesByInterval {

			if len(logSourceIds) == 0 {
				logger.Info("No log sources found for interval", zap.Int("interval", interval))
				continue
			}

			logger.Info("Fetching last event times for log sources in interval",
				zap.Int("interval", interval),
				zap.Strings("logsourceIds", logSourceIds))

			if len(logSourceIds) > 100 {
				logger.Warn("Too many log sources for interval, chunking",
					zap.Int("interval", interval),
					zap.Int("logSourceCount", len(logSourceIds)))

				chunkSize := 100
				for i := 0; i < len(logSourceIds); i += chunkSize {
					end := i + chunkSize
					if end > len(logSourceIds) {
						end = len(logSourceIds)
					}
					chunk := logSourceIds[i:end]
					sourceIdToLastEventTime, err := getSourceIdToLastEventTimeNew(ctx, osClient, tenantId, chunk)
					if err != nil {
						logger.Error("error getting last event times for tenant's log sources in interval",
							zap.Error(err),
							zap.Int("interval", interval))
						continue
					}

					for sourceId, lastTime := range sourceIdToLastEventTime {
						sourceIdToLastEventTime[sourceId] = lastTime
						logger.Debug("Retrieved last event time", zap.String("sourceId", sourceId), zap.Time("lastEventTime", lastTime))
					}
				}
				continue
			}

			sourceIdToLastEventTime, err := getSourceIdToLastEventTimeNew(ctx, osClient, tenantId, logSourceIds)
			if err != nil {
				logger.Error("error getting last event times for tenant's log sources in interval",
					zap.Error(err),
					zap.Int("interval", interval))
				continue
			}

			for sourceId, lastTime := range sourceIdToLastEventTime {
				sourceIdToLastEventTime[sourceId] = lastTime
				logger.Debug("Retrieved last event time", zap.String("sourceId", sourceId), zap.Time("lastEventTime", lastTime))
			}

		}
	}

	return nil

}
func CheckEntityStats(ctx context.Context) error {
	endTime := time.Now()
	db := config.GetDB()
	configMap, err := helper.CreateEntityAlertsConfigMapByType(db)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error creating EntityAlertsConfig map", zap.Error(err))
		return err
	}

	intervalCountMap := populateIntervalCountMap(configMap)

	updatedConfigs := make(map[uuid.UUID]time.Time)
	var entitiesToAlert []helper.EntityAlertsConfig

	for intervalInMinutes := range intervalCountMap {

		interval := time.Duration(intervalInMinutes) * time.Minute
		startTime := endTime.Add(-interval)

		filteredConfigMap := filterConfigsByInterval(configMap, intervalInMinutes)

		aggObj, err := getAggStatsForLogSourcePaginated(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error getting stats by interval", zap.Error(err))
			return err
		}
		logging.GetLoggerWithContext(ctx).Info("got response from statistics store", zap.Any("interval", interval.Minutes()), zap.Any("response", aggObj))

		updated, alerted := compareResultsAndUpdate(filteredConfigMap, aggObj, interval, startTime)
		for entityID, lastCheckedTime := range updated {
			updatedConfigs[entityID] = lastCheckedTime
		}

		logging.GetLoggerWithContext(ctx).Info("updated stats", zap.Any("updatedConfigs", updatedConfigs))

		entitiesToAlert = append(entitiesToAlert, alerted...)
	}

	// Update LastCheckedTime in DB for entities with data
	if len(updatedConfigs) > 0 {
		err = updateLastCheckedTimeInDB(ctx, db, updatedConfigs)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error updating LastCheckedTime in database", zap.Error(err), zap.Any("updates", updatedConfigs))
			return err
		}
		logging.GetLoggerWithContext(ctx).Info("Successfully updated LastCheckedTime in database", zap.Any("count", len(updatedConfigs)))
	}

	logging.GetLoggerWithContext(ctx).Info("Entities to alert (LastCheckedTime not updated):", zap.Any("count", len(entitiesToAlert)))
	return nil
}
func updateLastCheckedTimeInDB(ctx context.Context, db *gorm.DB, updatedConfigs map[uuid.UUID]time.Time) error {
	for entityID, lastCheckedTime := range updatedConfigs {
		err := db.Model(&helper.EntityAlertsConfig{}).Where("entity_id = ?", entityID).Update("last_checked_time", lastCheckedTime).Error
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error updating LastCheckedTime for entity", zap.Error(err), zap.String("entityId", entityID.String()), zap.Time("lastCheckedTime", lastCheckedTime))
			return err
		}
	}
	return nil
}

func filterConfigsByInterval(configMap map[string]helper.EntityAlertsConfig, interValInMinutes int) map[string]helper.EntityAlertsConfig {
	filtered := make(map[string]helper.EntityAlertsConfig)
	for key, config := range configMap {
		if config.Interval == interValInMinutes {
			filtered[key] = config
		}
	}
	return filtered
}

func populateIntervalCountMap(configMap map[string]helper.EntityAlertsConfig) map[int]int {
	intervalCountMap := make(map[int]int)
	for _, conf := range configMap {
		intervalCountMap[conf.Interval]++
	}
	return intervalCountMap
}

func compareResultsAndUpdate(configMap map[string]helper.EntityAlertsConfig, aggObj statistics.AggregateResponse, interval time.Duration, startTime time.Time) (map[uuid.UUID]time.Time, []helper.EntityAlertsConfig) {
	updatedConfigs := make(map[uuid.UUID]time.Time)
	var entitiesToAlert []helper.EntityAlertsConfig
	currentTime := time.Now()
	logSourceIdsStatsReceived := make(map[string]bool)

	for key, value := range aggObj.Agg {
		valueInt, ok := value.(float64)
		if ok && valueInt > 0 {
			if _, err := uuid.Parse(key); err == nil {
				logSourceIdsStatsReceived[key] = true
			}
		}
	}

	for _, configmap := range configMap {
		sourceId := configmap.EntityID.String()
		if configmap.Interval == int(interval.Minutes()) {
			if logSourceIdsStatsReceived[sourceId] {
				updatedConfigs[configmap.EntityID] = currentTime
				logging.GetLogger().Info("Data received, Added LastCheckedTime for ", zap.String("entityId", sourceId), zap.Time("lastCheckedTime", currentTime))
			} else {
				alertThreshold := configmap.LastCheckedTime.Add(interval)
				if startTime.After(alertThreshold) && configmap.Status {
					entitiesToAlert = append(entitiesToAlert, configmap)
					logging.GetLogger().Warn("Inactivity detected", zap.String("entityId", sourceId), zap.Time("lastCheckedTime", configmap.LastCheckedTime), zap.Time("alertThreshold", alertThreshold), zap.Time("startTime", startTime))
				} else {
					logging.GetLogger().Debug("No data, but within alert threshold", zap.String("entityId", sourceId), zap.Time("lastCheckedTime", configmap.LastCheckedTime), zap.Time("alertThreshold", alertThreshold), zap.Time("startTime", startTime))
				}
			}
		}
	}
	return updatedConfigs, entitiesToAlert
}
func getSourceIdToLastEventTimeNew(ctx context.Context, osClient *opensearch.Client, tenantId string, logsourceIds []string) (map[string]time.Time, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)
	aggFunc := os.AggregationFunction{
		Function: "max",
		Field:    "tags.db_ts_win",
		Name:     "last_event_time",
	}
	sourceIdToLastEventTime := make(map[string]time.Time)
	var after map[string]any = nil
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered" AND tags.db_event_source_id.keyword:(` + strings.Join(logsourceIds, " OR ") + `)`

	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, osClient, 100, statsAlias, q, []string{"tags.db_event_source_id.keyword"}, []os.AggregationFunction{aggFunc}, after)
		if err != nil {
			logging.GetLogger().Error("error while getting last event times from open search", zap.Error(err))
			return nil, err
		}
		if len(responses) == 0 {
			break
		}
		for _, response := range responses {
			sourceId := response.Key["tags.db_event_source_id.keyword"].(string)
			lastEventMillis := int64(response.Values["last_event_time"].(float64))
			lastEventTime := time.UnixMilli(lastEventMillis).UTC()
			sourceIdToLastEventTime[sourceId] = lastEventTime
		}
		after = newAfter
	}
	return sourceIdToLastEventTime, nil
}
