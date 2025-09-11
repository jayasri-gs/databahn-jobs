package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/constants"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
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

/*
AlertForNotEventsFromSources
This function checks for inactive sources and sends alerts to OpsGenie if the source is inactive for a specified alert interval.

	  For each tenant
		get all source to last event time map
		get all status:active sources paginated
		for each page
			read sources' alert config interval
			get last events of sources
			get inactive sources event time is older than alert config interval
			get active sources with event time within alert config interval
			for each inactive source
				create in app alert
			for active sources
				check if alert exists in app
					if yes, then dismiss alert
*/

const defaultAlertDuration30Min = 30 * time.Minute
const defaultRequiredEventsInLastSevenDays = 7 * 24 * time.Hour

func AlertForNoEventsFromSources(ctx context.Context) common.JobResult {
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

		sources, err := getAllLogSourcesOfTenant(db, tenantUuid)
		if err != nil {
			errorMsg := fmt.Sprintf("error while getting all log sources of tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error while getting all log sources of tenant", zap.Error(err), zap.String("tenantId", tenantId))
			return common.NewJobResultFromErrors(jobErrors)
		}

		if len(sources) == 0 {
			continue
		}

		logger.GetLogger().Info("checking for inactive sources", zap.String("tenantId", tenantId))

		sourceIdToLastEventTime, err := getSourceIdToLastEventTime(tenantId, ctx, osClient, sources)
		if err != nil {
			errorMsg := fmt.Sprintf("error getting source event times for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error getting source event times", zap.Error(err), zap.String("tenantId", tenantId))
			return common.NewJobResultFromErrors(jobErrors)
		}

		sourcesToAlert, activeSources, err := findInactiveAndActiveSources(db, tenantUuid, sourceIdToLastEventTime)
		if err != nil {
			errorMsg := fmt.Sprintf("error finding inactive sources for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error finding inactive sources", zap.Error(err), zap.String("tenantId", tenantId))
			return common.NewJobResultFromErrors(jobErrors)
		}

		if len(sourcesToAlert) == 0 {
			logger.GetLogger().Info("no sources to alert for tenant", zap.String("tenantId", tenantId))
		} else {
			//raise in app alerts
			err = sendInAppAlerts(sourcesToAlert, alertsManager)
			if err != nil {
				errorMsg := fmt.Sprintf("error sending in-app alerts for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error sending in-app alerts", zap.Error(err), zap.String("tenantId", tenantId))
				return common.NewJobResultFromErrors(jobErrors)
			}
		}

		//dismiss alerts if exists and data has come
		activeSourcesById := make(map[string]*source.Source)
		for _, src := range activeSources {
			activeSourcesById[src.ID.String()] = src
		}
		activeSourcesPartitions := util.PartitionSlice(activeSources, 10)
		var alertsToDismiss []string
		for _, activeSourcesPartition := range activeSourcesPartitions {
			sourceIds := make([]string, len(activeSourcesPartition))
			for i, s := range activeSourcesPartition {
				sourceIds[i] = s.ID.String()
			}
			q := "tenantId:" + tenantId + " AND dismissed:false AND functionalityEntityId:" + "(" + strings.Join(sourceIds, " OR ") + ")" + ` AND functionalityType:(` + constants.IngestionCheckerFunctionalityType + " OR " + alerts_async.IngestionChecker.String() + ")"
			openAlerts, _, err := os.Search(ctx, osClient, common.AlertsIndex, q)
			if err != nil {
				errorMsg := fmt.Sprintf("error searching for alerts for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error while searching for alerts", zap.Error(err), zap.String("query", q), zap.String("tenantId", tenantId))
				return common.NewJobResultFromErrors(jobErrors)
			}
			var alerts []statistics.AlertDocument
			decoder, err := util.CreateAlertDecoder(&alerts)
			if err != nil {
				errorMsg := fmt.Sprintf("error creating decoder for alerts for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error while creating decoder for alerts", zap.Error(err), zap.String("tenantId", tenantId))
				return common.NewJobResultFromErrors(jobErrors)
			}
			err = decoder.Decode(openAlerts)
			if err != nil {
				errorMsg := fmt.Sprintf("error decoding openSearch response for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLoggerWithContext(ctx).Error("error while decoding openSearch response", zap.Error(err), zap.String("tenantId", tenantId))
				return common.NewJobResultFromErrors(jobErrors)
			}
			if len(alerts) == 0 {
				logger.GetLogger().Info("no inactivity alerts found for tenant for active source", zap.String("tenantId", tenantId), zap.Any("sources", sourceIds))
				continue
			}
			for _, alrt := range alerts {
				if _, ok := activeSourcesById[alrt.FunctionalityEntityId]; ok {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
		}
		if len(alertsToDismiss) > 0 {
			err = alertsManager.AutoResolveAlerts(alertsToDismiss)
			if err != nil {
				errorMsg := fmt.Sprintf("error while dismissing alerts for tenant %s: %v", tenantId, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error while dismissing alerts", zap.Error(err), zap.String("tenantId", tenantId))
				return common.NewJobResultFromErrors(jobErrors)
			}
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("successfully completed source inactivity check")
		return common.NewJobResultSuccess()
	} else {
		logger.GetLogger().Info("source inactivity check completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}

func sendInAppAlerts(sourcesToAlert []*model.InActiveSource, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(sourcesToAlert))
	for i, srcAlert := range sourcesToAlert {
		newAlert, err := buildAlert(*srcAlert)
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

func findInactiveAndActiveSources(db *gorm.DB, tenantUuid uuid.UUID, sourceIdToLastEventTime map[string]time.Time) ([]*model.InActiveSource, []*source.Source, error) {
	var sourcesToAlert []*model.InActiveSource
	var activeSources []*source.Source

	sourceDbPage := 0
	sourceDbPageSize := 50

	for {
		sources, err := readSourcesPaginated(db, tenantUuid, sourceDbPage, sourceDbPageSize)
		if err != nil {
			logger.GetLogger().Error("error while reading sources", zap.Error(err))
			return nil, nil, err
		}
		var sourceIds []uuid.UUID
		for _, s := range sources {
			sourceIds = append(sourceIds, s.ID)
		}
		if len(sourceIds) == 0 {
			break
		}
		alertConfigs, err := entities.ReadEntityConfigs(db, entities.LogSourceEntityType, "LOG_SOURCE_INACTIVITY", tenantUuid, sourceIds)
		if err != nil {
			logger.GetLogger().Error("error while reading source entity configs", zap.Error(err))
			return nil, nil, err
		}
		alertConfigsBySourceId := make(map[uuid.UUID]entities.EntityAlertsConfig)
		for _, alertConfig := range alertConfigs {
			alertConfigsBySourceId[alertConfig.EntityID] = alertConfig
		}

		for _, s := range sources {
			sourceId := s.ID.String()
			lastEventTime, ok := sourceIdToLastEventTime[sourceId]
			if !ok {
				logger.GetLogger().Warn("no last event time found for source, ignoring", zap.String("sourceId", sourceId), zap.String("tenantId", tenantUuid.String()))
				continue
			}
			alertConfig, ok := alertConfigsBySourceId[s.ID]
			var alertDuration time.Duration
			if ok {
				configuredDuration, errr, skip := getAlertDuration(alertConfig, sourceId, tenantUuid.String())
				if errr != nil {
					logger.GetLogger().Error("error while getting alert duration, ignoring", zap.Error(errr), zap.String("sourceId", sourceId), zap.String("tenantId", tenantUuid.String()))
					continue
				}
				if skip {
					logger.GetLogger().Warn("alert config is disabled, skipping", zap.String("sourceId", sourceId), zap.String("tenantId", tenantUuid.String()))
					continue
				}
				alertDuration = configuredDuration
			} else {
				logger.GetLogger().Warn("no alert config found for source, defaulting", zap.String("sourceId", sourceId), zap.String("tenantId", tenantUuid.String()))
				alertDuration = defaultAlertDuration30Min
			}
			now := time.Now().UTC()
			if now.Sub(lastEventTime) > alertDuration {
				logger.GetLogger().Info("source received data more than alert duration ago, eligible for alert", zap.String("sourceId", sourceId), zap.String("tenantId", tenantUuid.String()), zap.Duration("alertDuration", alertDuration), zap.Time("lastEventTime", lastEventTime))
				if lastEventTime.Before(now.Add(-defaultRequiredEventsInLastSevenDays)) {
					logger.GetLogger().Info("source received data older than 7 days, not eligible for alert", zap.String("sourceId", sourceId), zap.String("tenantId", tenantUuid.String()), zap.Duration("alertDuration", alertDuration), zap.Time("lastEventTime", lastEventTime))
					continue
				}
				ias := model.NewInActiveSource(&s, lastEventTime, alertDuration)
				sourcesToAlert = append(sourcesToAlert, ias)
			} else {
				logger.GetLogger().Info("source received data within alert duration, not eligible for alert", zap.String("sourceId", sourceId), zap.String("tenantId", tenantUuid.String()), zap.Duration("alertDuration", alertDuration), zap.Time("lastEventTime", lastEventTime))
				activeSources = append(activeSources, &s)
			}
		}
		sourceDbPage += 1
	}
	return sourcesToAlert, activeSources, nil
}

func getSourceIdToLastEventTime(tenantId string, ctx context.Context, osClient *opensearch.Client, sources []source.Source) (map[string]time.Time, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)
	aggFunc := os.AggregationFunction{
		Function: "max",
		Field:    "tags.db_ts_win",
		Name:     "last_event_time",
	}
	sourceIdToLastEventTime := make(map[string]time.Time)
	var after map[string]any = nil
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, osClient, 100, statsAlias, q, []string{"tags.db_event_source_id.keyword"}, []os.AggregationFunction{aggFunc}, after)
		if err != nil {
			logger.GetLogger().Error("error while getting last event times from open search", zap.Error(err))
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
	logger.GetLoggerWithContext(ctx).Debug("Checking for sources that were newly onboarded and updating them in open search map")
	for _, s := range sources {
		if _, ok := sourceIdToLastEventTime[s.ID.String()]; !ok {
			sourceIdToLastEventTime[s.ID.String()] = s.UpdatedAt.UTC()
		}
	}

	return sourceIdToLastEventTime, nil
}

func readSourcesPaginated(db *gorm.DB, tenantId uuid.UUID, page, pageSize int) ([]source.Source, error) {
	var sources []source.Source
	offset := page * pageSize

	result := db.
		Where("tenant_id = ? AND status = 'ACTIVE'", tenantId).
		Limit(pageSize).
		Offset(offset).
		Find(&sources)

	return sources, result.Error
}

func getAlertDuration(alertConfig entities.EntityAlertsConfig, sourceId, tenantId string) (time.Duration, error, bool) {
	dbConfig := alertConfig.Config
	if dbConfig == nil {
		logger.GetLogger().Warn("no alert config found for source, defaulting", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId))
		return defaultAlertDuration30Min, nil, false
	}
	if !dbConfig.Enabled {
		logger.GetLogger().Warn("alert config is disabled for source, not notifying", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId))
		return defaultAlertDuration30Min, nil, true
	}
	inactivityAlertConfig := dbConfig.LogSourceInactivityAlertConfig
	if inactivityAlertConfig == nil {
		logger.GetLogger().Warn("no alert config LogSourceInactivityAlertConfig found for source, defaulting", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId))
		return defaultAlertDuration30Min, nil, false
	}
	duration := inactivityAlertConfig.InactivityDuration
	if duration == nil {
		logger.GetLogger().Warn("no alert config LogSourceInactivityAlertConfig InactivityDuration found for source, defaulting", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId))
		return defaultAlertDuration30Min, nil, false
	}
	dur, err := duration.GetDuration()
	return dur, err, false
}

func buildAlert(ias model.InActiveSource) (*alerts_async.Alert, error) {
	title := fmt.Sprintf(constants.IngestionCheckerFunctionalityTitle, ias.InactivityDurationStr())
	message := fmt.Sprintf(constants.IngestionCheckerFunctionalityMessage, ias.AlertConfigDurationStr(),
		util.HumanReadableTimeWithZone(ias.CheckedAt), util.HumanReadableTimeWithZone(ias.LastEventTime))
	functionality := alerts_async.LogSource
	if ias.Source.Scope == "CLOUD" {
		functionality = alerts_async.CloudLogSource
	}
	return alerts_async.NewAlert(functionality,
		alerts_async.WithEntity(ias),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.IngestionChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10001, ""),
	)
}

func getAllLogSourcesOfTenant(db *gorm.DB, tenantId uuid.UUID) ([]source.Source, error) {
	var sources []source.Source
	result := db.Where("tenant_id = ? AND status = 'ACTIVE'", tenantId).Find(&sources)

	return sources, result.Error
}
