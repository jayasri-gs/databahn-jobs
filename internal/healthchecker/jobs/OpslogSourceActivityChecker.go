package jobs

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"io"
	"strconv"
	"strings"
	"time"
)

var predefinedIntervals = []time.Duration{
	15 * time.Minute, //INTERNAL DEV ONLY
	30 * time.Minute,
	60 * time.Minute,
	180 * time.Minute,
	360 * time.Minute,
	720 * time.Minute,
	1440 * time.Minute,
}

func GetStatsByInterval(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	agg := "tags.db_event_source_id.keyword"

	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, os.StatsIndex+"*", &searchBody, os.GetClient())

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("index", os.StatsIndex))
		return statistics.AggregateResponse{}, err
	}
	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewAggregateResponse(resp)
	return aggObj, err
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
	filteredIntervals := filterIntervals(intervalCountMap)

	updatedConfigs := make(map[uuid.UUID]time.Time)
	var entitiesToAlert []helper.EntityAlertsConfig

	for _, interval := range filteredIntervals {
		startTime := endTime.Add(-interval)
		aggObj, err := GetStatsByInterval(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error getting stats by interval", zap.Error(err))
			return err
		}
		logging.GetLoggerWithContext(ctx).Info("got response from statistics store", zap.Reflect("interval", interval.Minutes()), zap.Reflect("response", aggObj))

		updated, alerted := compareResultsAndUpdate(configMap, aggObj, interval, startTime)
		for entityID, lastCheckedTime := range updated {
			updatedConfigs[entityID] = lastCheckedTime
		}
		entitiesToAlert = append(entitiesToAlert, alerted...)
	}

	// Update LastCheckedTime in DB for entities with data
	if len(updatedConfigs) > 0 {
		err = updateLastCheckedTimeInDB(ctx, db, updatedConfigs)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error updating LastCheckedTime in database", zap.Error(err), zap.Reflect("updates", updatedConfigs))
			return err
		}
		logging.GetLoggerWithContext(ctx).Info("Successfully updated LastCheckedTime in database", zap.Reflect("count", len(updatedConfigs)))
	}

	logging.GetLoggerWithContext(ctx).Info("Entities to alert (LastCheckedTime not updated):", zap.Reflect("count", len(entitiesToAlert)))
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

func populateIntervalCountMap(configMap map[string]helper.EntityAlertsConfig) map[int]int {
	intervalCountMap := make(map[int]int)
	for _, conf := range configMap {
		intervalCountMap[conf.Interval]++
	}
	return intervalCountMap
}

func filterIntervals(intervalCountMap map[int]int) []time.Duration {
	var filteredIntervals []time.Duration
	for _, interval := range predefinedIntervals {
		if intervalCountMap[int(interval.Minutes())] > 0 {
			filteredIntervals = append(filteredIntervals, interval)
		} else {
			logging.GetLogger().Info("No entities found for interval", zap.Reflect("interval", interval.Minutes()))
		}
	}
	return filteredIntervals
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
				logging.GetLogger().Info("Data received, updated LastCheckedTime in memory", zap.String("entityId", sourceId), zap.Time("lastCheckedTime", currentTime))
			} else {
				alertThreshold := configmap.LastCheckedTime.Add(interval)
				if startTime.After(alertThreshold) && configmap.Status {
					entitiesToAlert = append(entitiesToAlert, configmap)
					logging.GetLogger().Warn("Inactivity detected, added to alert list", zap.String("entityId", sourceId), zap.Time("lastCheckedTime", configmap.LastCheckedTime), zap.Time("alertThreshold", alertThreshold), zap.Time("startTime", startTime))
				} else {
					logging.GetLogger().Debug("No data, but within alert threshold", zap.String("entityId", sourceId), zap.Time("lastCheckedTime", configmap.LastCheckedTime), zap.Time("alertThreshold", alertThreshold), zap.Time("startTime", startTime))
				}
			}
		}
	}
	return updatedConfigs, entitiesToAlert
}
