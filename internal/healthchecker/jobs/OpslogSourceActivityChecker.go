package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
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
					sourceIdToLastEventTime, err := getSourceIdToLastEventTimeNew(ctx, osClient, tenantId, interval, chunk)
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

			sourceIdToLastEventTime, err := getSourceIdToLastEventTimeNew(ctx, osClient, tenantId, interval, logSourceIds)
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
			if len(sourceIdToLastEventTime) > 0 {
				err = updateLastCheckedTimeInDB(ctx, db, sourceIdToLastEventTime)
				if err != nil {
					logger.Error("error updating LastCheckedTime in database",
						zap.Error(err),
						zap.Any("sourceIdToLastEventTime", sourceIdToLastEventTime))
					return err
				}
			}

		}

	}

	return nil

}

func updateLastCheckedTimeInDB(ctx context.Context, db *gorm.DB, sourceIdToLastEventTime map[string]time.Time) error {
	for entityID, lastCheckedTime := range sourceIdToLastEventTime {
		err := db.Model(&helper.EntityAlertsConfig{}).Where("entity_id = ?", entityID).Update("last_checked_time", lastCheckedTime).Error
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error updating LastCheckedTime for entity", zap.Error(err), zap.String("entityId", entityID), zap.Time("lastCheckedTime", lastCheckedTime))
			return err
		}
	}
	return nil
}

func getSourceIdToLastEventTimeNew(ctx context.Context, osClient *opensearch.Client, tenantId string, interval int, logsourceIds []string) (map[string]time.Time, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)
	aggFunc := os.AggregationFunction{
		Function: "max",
		Field:    "tags.db_ts_win",
		Name:     "last_event_time",
	}
	sourceIdToLastEventTime := make(map[string]time.Time)
	var after map[string]any = nil
	intervalStr := fmt.Sprintf("now-%dm", interval)
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered" AND tags.db_event_source_id.keyword:(` + strings.Join(logsourceIds, " OR ") + `) AND tags.db_ts_win:>=` + intervalStr

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
			logging.GetLoggerWithContext(ctx).Info("Last event time for", zap.Any("logsourceId", sourceId), zap.Any("lastEventTime ", lastEventTime))
		}
		after = newAfter
	}
	return sourceIdToLastEventTime, nil
}
