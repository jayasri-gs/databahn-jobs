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

		sourceFleetLastEventTimes, err := getSourceFleetLastEventTimes(tenantId, ctx, osClient)
		if err != nil {
			errorMsg := fmt.Sprintf("error getting source-fleet event times for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error getting source-fleet event times", zap.Error(err), zap.String("tenantId", tenantId))
			return common.NewJobResultFromErrors(jobErrors)
		}

		sourceAgentLastEventTimes, err := getSourceAgentLastEventTimes(tenantId, ctx, osClient)
		if err != nil {
			errorMsg := fmt.Sprintf("error getting source-agent event times for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error getting source-agent event times", zap.Error(err), zap.String("tenantId", tenantId))
			return common.NewJobResultFromErrors(jobErrors)
		}

		sourcesToAlert, activeSources, err := findInactiveAndActiveSources(db, tenantUuid, sourceIdToLastEventTime, sourceFleetLastEventTimes, sourceAgentLastEventTimes)
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

// sourceFleetKey returns a composite key for (sourceId, fleetId) lookups
func sourceFleetKey(sourceId, fleetId string) string {
	return sourceId + "|" + fleetId
}

// sourceAgentKey returns a composite key for (sourceId, agentId) lookups
func sourceAgentKey(sourceId, agentId string) string {
	return sourceId + "|" + agentId
}

func findInactiveAndActiveSources(db *gorm.DB, tenantUuid uuid.UUID, sourceIdToLastEventTime map[string]time.Time, sourceFleetLastEventTimes map[string]time.Time, sourceAgentLastEventTimes map[string]time.Time) ([]*model.InActiveSource, []*source.Source, error) {
	var sourcesToAlert []*model.InActiveSource
	var activeSources []*source.Source

	sourceDbPage := 0
	sourceDbPageSize := 50

	for {
		sources, err := source.ReadSourcesPaginated(db, tenantUuid, sourceDbPage, sourceDbPageSize, []string{"ACTIVE", "ERRORED", "DEPLOYING"})
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

		// For FLEET-scoped sources, fetch fleet associations
		var fleetScopedSourceIds []uuid.UUID
		for _, s := range sources {
			if s.Scope == "FLEET" {
				fleetScopedSourceIds = append(fleetScopedSourceIds, s.ID)
			}
		}
		sourceToFleets := make(map[string][]source.SourceFleetInfo)
		if len(fleetScopedSourceIds) > 0 {
			sourceToFleets, err = source.GetFleetsBySourceIDs(db, fleetScopedSourceIds)
			if err != nil {
				logger.GetLogger().Error("error while getting fleet associations for sources", zap.Error(err), zap.String("tenantId", tenantUuid.String()))
				return nil, nil, err
			}
		}

		// For AGENT-scoped sources, fetch agent associations
		var agentScopedSourceIds []uuid.UUID
		for _, s := range sources {
			if s.Scope == "AGENT" {
				agentScopedSourceIds = append(agentScopedSourceIds, s.ID)
			}
		}
		sourceToAgents := make(map[string][]source.SourceAgentInfo)
		if len(agentScopedSourceIds) > 0 {
			sourceToAgents, err = source.GetAgentsBySourceIDs(db, agentScopedSourceIds)
			if err != nil {
				logger.GetLogger().Error("error while getting agent associations for sources", zap.Error(err), zap.String("tenantId", tenantUuid.String()))
				return nil, nil, err
			}
		}

		for _, s := range sources {
			sourceId := s.ID.String()
			tenantId := tenantUuid.String()

			alertConfig, ok := alertConfigsBySourceId[s.ID]
			var alertDuration time.Duration
			if ok {
				configuredDuration, errr, skip := getAlertDuration(alertConfig, sourceId, tenantId)
				if errr != nil {
					logger.GetLogger().Error("error while getting alert duration, ignoring", zap.Error(errr), zap.String("sourceId", sourceId), zap.String("tenantId", tenantId))
					continue
				}
				if skip {
					logger.GetLogger().Warn("alert config is disabled, skipping", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId))
					continue
				}
				alertDuration = configuredDuration
			} else {
				logger.GetLogger().Warn("no alert config found for source, defaulting", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId))
				alertDuration = defaultAlertDuration30Min
			}

			fleets := sourceToFleets[sourceId]
			agents := sourceToAgents[sourceId]
			if s.Scope == "FLEET" && len(fleets) > 1 {
				logger.GetLogger().Info("multi-fleet source detected, checking per-fleet inactivity", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId), zap.Int("fleetCount", len(fleets)))
				allFleetsActive := true
				for _, fi := range fleets {
					key := sourceFleetKey(sourceId, fi.FleetId)
					fleetLastEventTime, found := sourceFleetLastEventTimes[key]
					if !found {
						fleetLastEventTime = s.UpdatedAt.UTC()
					}
					now := time.Now().UTC()
					if now.Sub(fleetLastEventTime) > alertDuration {
						if fleetLastEventTime.Before(now.Add(-defaultRequiredEventsInLastSevenDays)) {
							logger.GetLogger().Info("multi-fleet source fleet data older than 7 days, skipping", zap.String("sourceId", sourceId), zap.String("fleetId", fi.FleetId), zap.String("tenantId", tenantId))
							continue
						}
						logger.GetLogger().Info("multi-fleet source inactive on fleet, eligible for alert", zap.String("sourceId", sourceId), zap.String("fleetId", fi.FleetId), zap.String("fleetName", fi.FleetName), zap.String("tenantId", tenantId), zap.Time("lastEventTime", fleetLastEventTime))
						ias := model.NewInActiveSourceWithFleet(&s, fleetLastEventTime, alertDuration, fi.FleetId, fi.FleetName)
						sourcesToAlert = append(sourcesToAlert, ias)
						allFleetsActive = false
					} else {
						logger.GetLogger().Debug("multi-fleet source active on fleet", zap.String("sourceId", sourceId), zap.String("fleetId", fi.FleetId), zap.String("tenantId", tenantId))
					}
				}
				if allFleetsActive {
					activeSources = append(activeSources, &s)
				}
			} else if s.Scope == "AGENT" && len(agents) > 1 {
				logger.GetLogger().Info("multi-agent source detected, checking per-agent inactivity", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId), zap.Int("agentCount", len(agents)))
				allAgentsActive := true
				for _, ai := range agents {
					key := sourceAgentKey(sourceId, ai.AgentId)
					agentLastEventTime, found := sourceAgentLastEventTimes[key]
					if !found {
						agentLastEventTime = s.UpdatedAt.UTC()
					}
					now := time.Now().UTC()
					if now.Sub(agentLastEventTime) > alertDuration {
						if agentLastEventTime.Before(now.Add(-defaultRequiredEventsInLastSevenDays)) {
							logger.GetLogger().Info("multi-agent source agent data older than 7 days, skipping", zap.String("sourceId", sourceId), zap.String("agentId", ai.AgentId), zap.String("tenantId", tenantId))
							continue
						}
						logger.GetLogger().Info("multi-agent source inactive on agent, eligible for alert", zap.String("sourceId", sourceId), zap.String("agentId", ai.AgentId), zap.String("agentName", ai.AgentName), zap.String("tenantId", tenantId), zap.Time("lastEventTime", agentLastEventTime))
						ias := model.NewInActiveSourceWithAgent(&s, agentLastEventTime, alertDuration, ai.AgentId, ai.AgentName)
						sourcesToAlert = append(sourcesToAlert, ias)
						allAgentsActive = false
					} else {
						logger.GetLogger().Debug("multi-agent source active on agent", zap.String("sourceId", sourceId), zap.String("agentId", ai.AgentId), zap.String("tenantId", tenantId))
					}
				}
				if allAgentsActive {
					activeSources = append(activeSources, &s)
				}
			} else {
				lastEventTime, found := sourceIdToLastEventTime[sourceId]
				if !found {
					logger.GetLogger().Warn("no last event time found for source, ignoring", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId))
					continue
				}
				now := time.Now().UTC()
				if now.Sub(lastEventTime) > alertDuration {
					logger.GetLogger().Info("source received data more than alert duration ago, eligible for alert", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId), zap.Duration("alertDuration", alertDuration), zap.Time("lastEventTime", lastEventTime))
					if lastEventTime.Before(now.Add(-defaultRequiredEventsInLastSevenDays)) {
						logger.GetLogger().Info("source received data older than 7 days, not eligible for alert", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId), zap.Duration("alertDuration", alertDuration), zap.Time("lastEventTime", lastEventTime))
						continue
					}
					ias := model.NewInActiveSource(&s, lastEventTime, alertDuration)
					sourcesToAlert = append(sourcesToAlert, ias)
				} else {
					logger.GetLogger().Info("source received data within alert duration, not eligible for alert", zap.String("sourceId", sourceId), zap.String("tenantId", tenantId), zap.Duration("alertDuration", alertDuration), zap.Time("lastEventTime", lastEventTime))
					activeSources = append(activeSources, &s)
				}
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

// getSourceFleetLastEventTimes returns last event times keyed by "sourceId|fleetId"
// by aggregating on both db_event_source_id and db_fleet_id dimensions in OpenSearch.
func getSourceFleetLastEventTimes(tenantId string, ctx context.Context, osClient *opensearch.Client) (map[string]time.Time, error) {
	statsAlias := os.StatisticsIndexAlias(tenantId)
	aggFunc := os.AggregationFunction{
		Function: "max",
		Field:    "tags.db_ts_win",
		Name:     "last_event_time",
	}
	result := make(map[string]time.Time)
	var after map[string]any = nil
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, osClient, 100, statsAlias, q,
			[]string{"tags.db_event_source_id.keyword", "tags.db_fleet_id.keyword"},
			[]os.AggregationFunction{aggFunc}, after)
		if err != nil {
			logger.GetLogger().Error("error while getting source-fleet last event times from opensearch", zap.Error(err))
			return nil, err
		}
		if len(responses) == 0 {
			break
		}
		for _, response := range responses {
			sourceId := response.Key["tags.db_event_source_id.keyword"].(string)
			fleetId := response.Key["tags.db_fleet_id.keyword"].(string)
			if fleetId == "" {
				continue
			}
			lastEventMillis := int64(response.Values["last_event_time"].(float64))
			lastEventTime := time.UnixMilli(lastEventMillis).UTC()
			result[sourceFleetKey(sourceId, fleetId)] = lastEventTime
		}
		after = newAfter
	}
	logger.GetLoggerWithContext(ctx).Debug("fetched source-fleet last event times", zap.Int("entries", len(result)))
	return result, nil
}

// getSourceAgentLastEventTimes returns last event times keyed by "sourceId|agentId"
// by aggregating on both db_event_source_id and db_agent_id dimensions in the agent-ingestion metrics.
// Agent metrics live in the shared "db_statistics_agent" index (not per-tenant), so the query
// must include a tenant filter.
func getSourceAgentLastEventTimes(tenantId string, ctx context.Context, osClient *opensearch.Client) (map[string]time.Time, error) {
	const agentStatsIndex = "db_statistics_agent"
	aggFunc := os.AggregationFunction{
		Function: "max",
		Field:    "tags.db_ts_win",
		Name:     "last_event_time",
	}
	result := make(map[string]time.Time)
	var after map[string]any = nil
	q := fmt.Sprintf(`tags.db_tenant_id: %q AND tags.component_name: "agent-ingestion" AND name: "total_data_received"`, tenantId)
	for {
		responses, newAfter, err := os.CompositePaginatedAggregate(ctx, osClient, 100, agentStatsIndex, q,
			[]string{"tags.db_event_source_id.keyword", "tags.db_agent_id.keyword"},
			[]os.AggregationFunction{aggFunc}, after)
		if err != nil {
			logger.GetLogger().Error("error while getting source-agent last event times from opensearch", zap.Error(err))
			return nil, err
		}
		if len(responses) == 0 {
			break
		}
		for _, response := range responses {
			sourceId := response.Key["tags.db_event_source_id.keyword"].(string)
			agentId := response.Key["tags.db_agent_id.keyword"].(string)
			if agentId == "" {
				continue
			}
			lastEventMillis := int64(response.Values["last_event_time"].(float64))
			lastEventTime := time.UnixMilli(lastEventMillis).UTC()
			result[sourceAgentKey(sourceId, agentId)] = lastEventTime
		}
		after = newAfter
	}
	logger.GetLoggerWithContext(ctx).Debug("fetched source-agent last event times", zap.Int("entries", len(result)))
	return result, nil
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
	if ias.IsFleetScoped() {
		message = fmt.Sprintf("This source is configured to be alerted on not receiving data in last %s for fleet '%s'. As of '%s' last event from this fleet was received at '%s'.",
			ias.AlertConfigDurationStr(), ias.FleetName,
			util.HumanReadableTimeWithZone(ias.CheckedAt), util.HumanReadableTimeWithZone(ias.LastEventTime))
	} else if ias.IsAgentScoped() {
		message = fmt.Sprintf("This source is configured to be alerted on not receiving data in last %s for agent '%s'. As of '%s' last event from this agent was received at '%s'.",
			ias.AlertConfigDurationStr(), ias.AgentName,
			util.HumanReadableTimeWithZone(ias.CheckedAt), util.HumanReadableTimeWithZone(ias.LastEventTime))
	}
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
		alerts_async.WithAction("Please check log source, fleet, connector etc of the source and devices sending data to the source."),
	)
}

func getAllLogSourcesOfTenant(db *gorm.DB, tenantId uuid.UUID) ([]source.Source, error) {
	var sources []source.Source
	result := db.Where("tenant_id = ? AND status IN ('ACTIVE', 'ERRORED', 'DEPLOYING')", tenantId).Find(&sources)

	return sources, result.Error
}
