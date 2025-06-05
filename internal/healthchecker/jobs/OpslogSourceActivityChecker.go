package jobs

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strconv"
	"time"
)

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
