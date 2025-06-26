package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"github.com/databahn-ai/databahn-jobs/internal/store/fleet"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	StatusDeleted                 = "DELETED"
	EnvFleetHealthCheckTime       = "FLEET_HEALTH_CHECK_TIME"
	EnvAgentHealthCheckTime       = "AGENT_HEALTH_CHECK_TIME"
	EnvFleetHealthCheckIgnoreTime = "FLEET_HEALTH_CHECK_IGNORE_TIME"
)

var AgentHealthCheckTimeNew = utils.GetEnvInt(EnvAgentHealthCheckTime, 10)
var FleetHealthCheckTimeNew = utils.GetEnvInt(EnvFleetHealthCheckTime, 10)
var FleetHealthCheckIgnoreTimeNew = utils.GetEnvInt(EnvFleetHealthCheckIgnoreTime, 600)

func HealthCheckJob(ctx context.Context) error {

	logger.GetLoggerWithContext(ctx).Info("starting health check job")

	db := config.GetDB()
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		return err
	}
	defer alertsManager.Close(ctx)

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLogger().Error("error while fetching tenants", zap.Error(err))
		return err
	}

	logger.GetLoggerWithContext(ctx).Info("Starting health check for unhealthy agents")

	for _, t := range tenants {

		err = alertForUnhealthyAgents(ctx, alertsManager, t.Id.String())
		if err != nil {
			logger.GetLogger().Error("error while handling alerts for unhealthy agents", zap.Error(err))
			return err
		}

		err = alertForFleetComponentsHealthCheck(ctx, alertsManager, t.Id.String())
		if err != nil {
			logger.GetLogger().Error("error while handling alerts for unhealthy fleet components", zap.Error(err))
			return err
		}

		logger.GetLoggerWithContext(ctx).Info("Starting health check for unhealthy fleet nodes")
		err = alertForFleetHealthCheck(ctx, alertsManager, t.Id.String())
		if err != nil {
			logger.GetLogger().Error("error while handling alerts for unhealthy fleet nodes", zap.Error(err))
			return err
		}

		logger.GetLoggerWithContext(ctx).Info("Starting health check for unhealthy fleet connectors")
		err = alertForFleetConnectorsHealthCheck(ctx, alertsManager, t.Id.String())
		if err != nil {
			logger.GetLogger().Error("error while handling alerts for unhealthy fleet connectors", zap.Error(err))
			return err
		}
	}

	logger.GetLoggerWithContext(ctx).Info("Starting health check for unhealthy fleet components")

	return nil
}

func alertForUnhealthyAgents(ctx context.Context, alertsManager *alert.AlertsManager, tenantId string) error {
	db := config.GetDB()

	healthCheckTime := time.Now().Add(-time.Minute * time.Duration(AgentHealthCheckTimeNew))
	page, pageSize := 0, 50
	for {
		activeAgents, inactiveAgents, err := findActiveAndInactiveAgents(db, tenantId, healthCheckTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive agents", zap.Error(err))
			return err
		}
		if len(inactiveAgents) == 0 && len(activeAgents) == 0 {
			logger.GetLogger().Info("no more inactive agents found", zap.Int("page", page))
			break
		}

		if len(inactiveAgents) > 0 {
			err = sendAgentInAppAlerts(inactiveAgents, alertsManager)
			if err != nil {
				logger.GetLogger().Error("error while sending agent in-app alerts", zap.Error(err))
				return err
			}
		}
		activeAgentsById := make(map[string]*agent.Agent)
		for _, ag := range activeAgents {
			activeAgentsById[ag.ID.String()] = ag
		}
		activeAgentsPartitions := util.PartitionSlice(activeAgents, 10)
		var alertsToDismiss []string

		for _, activeAgentsPartition := range activeAgentsPartitions {
			agentIds := make([]string, len(activeAgentsPartition))

			for i, a := range activeAgentsPartition {
				agentIds[i] = a.ID.String()
			}

			existingAlerts, err := getExistingAlerts(ctx, tenantId, agentIds, os.GetClient(), common.AgentHealthCheck)
			if err != nil {
				logger.GetLogger().Error("error while getting existing alerts for agents", zap.Error(err), zap.String("tenantId", tenantId))
				return err
			}
			if len(existingAlerts) == 0 {
				logger.GetLogger().Info("no existing alerts found for active agents", zap.Strings("agentIds", agentIds))
				continue
			}
			for _, alrt := range existingAlerts {
				if _, ok := activeAgentsById[alrt.FunctionalityEntityId]; ok {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}

		}

		if len(alertsToDismiss) > 0 {
			err = alertsManager.AutoResolveAlerts(alertsToDismiss)
			if err != nil {
				logger.GetLogger().Error("error while auto-resolving alerts for active agents", zap.Error(err))
				return err
			}
		}

		page++
	}
	return nil
}

func alertForFleetHealthCheck(ctx context.Context, alertsManager *alert.AlertsManager, tenantId string) error {
	db := config.GetDB()

	healthCheckTime := time.Now().Add(-time.Minute * time.Duration(FleetHealthCheckTimeNew))
	healthCheckIgnoreTime := time.Now().Add(-time.Minute * time.Duration(FleetHealthCheckIgnoreTimeNew))
	page, pageSize := 0, 50
	for {
		activeFleet, inactiveFleet, err := findActiveAndInactiveFleet(db, tenantId, healthCheckTime, healthCheckIgnoreTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive fleet nodes", zap.Error(err))
			return err
		}
		if len(inactiveFleet) == 0 && len(activeFleet) == 0 {
			logger.GetLogger().Info("no more inactive fleet nodes found", zap.Int("page", page))
			break
		}
		if len(inactiveFleet) > 0 {
			err = sendFleetInAppAlerts(inactiveFleet, alertsManager)
			if err != nil {
				logger.GetLogger().Error("error while sending fleet in-app alerts", zap.Error(err))
				return err
			}
		}

		activeFleetById := make(map[string]*fleet.Node)

		for _, af := range activeFleet {
			activeFleetById[af.Id.String()] = af
		}

		activeFleetPartitions := util.PartitionSlice(activeFleet, 10)
		var alertsToDismiss []string

		for _, activeFleetPartition := range activeFleetPartitions {
			fleetIds := make([]string, len(activeFleetPartition))
			for i, a := range activeFleetPartition {
				fleetIds[i] = a.Id.String()
			}

			existingAlerts, err := getExistingAlerts(ctx, tenantId, fleetIds, os.GetClient(), common.FleetNodeHealthCheck)
			if err != nil {
				logger.GetLogger().Error("error while getting existing alerts for fleet", zap.Error(err))
				return err
			}
			if len(existingAlerts) == 0 {
				logger.GetLogger().Info("no existing alerts found for fleet", zap.Strings("fleetIds", fleetIds))
				continue
			}
			for _, alrt := range existingAlerts {
				if _, ok := activeFleetById[alrt.FunctionalityEntityId]; ok {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
		}
		if len(alertsToDismiss) > 0 {
			err = alertsManager.AutoResolveAlerts(alertsToDismiss)
			if err != nil {
				logger.GetLogger().Error("error while auto-resolving alerts for fleet", zap.Error(err))
			}
		}

		page++
	}
	return nil
}

func alertForFleetConnectorsHealthCheck(ctx context.Context, alertsManager *alert.AlertsManager, tenantId string) error {
	db := config.GetDB()

	healthCheckTime := time.Now().Add(-time.Minute * time.Duration(FleetHealthCheckTimeNew))
	healthCheckIgnoreTime := time.Now().Add(-time.Minute * time.Duration(FleetHealthCheckIgnoreTimeNew))
	page, pageSize := 0, 50
	for {
		activeFleetConnectors, inactiveFleetConnectors, err := findActiveAndInactiveFleetConnectors(db, tenantId, healthCheckTime, healthCheckIgnoreTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive fleet connectors", zap.Error(err))
			return err
		}
		if len(inactiveFleetConnectors) == 0 && len(activeFleetConnectors) == 0 {
			logger.GetLogger().Info("no more inactive fleet connectors found", zap.Int("page", page))
			break
		}

		err = sendFleetConnectorInAppAlerts(inactiveFleetConnectors, alertsManager)
		if err != nil {
			logger.GetLogger().Error("error while sending fleet in-app alerts", zap.Error(err))
			return err
		}

		activeFleetConnectorById := make(map[string]*fleet.Connector)
		for _, af := range activeFleetConnectors {
			activeFleetConnectorById[af.ID.String()] = af
		}

		activeFleetConnectorPartitions := util.PartitionSlice(activeFleetConnectors, 10)
		var alertsToDismiss []string

		for _, activeFleetConnectorPartition := range activeFleetConnectorPartitions {
			fleetConnectorIds := make([]string, len(activeFleetConnectorPartition))
			for i, a := range activeFleetConnectorPartition {
				fleetConnectorIds[i] = a.ID.String()
			}

			existingAlerts, err := getExistingAlerts(ctx, tenantId, fleetConnectorIds, os.GetClient(), alerts_common.FleetConnectorHealthCheck)
			if err != nil {
				logger.GetLogger().Error("error while getting existing alerts for fleet", zap.Error(err))
			}

			if len(existingAlerts) == 0 {
				logger.GetLogger().Info("alerts not found for fleet connector", zap.Strings("fleetIds", fleetConnectorIds))
			}

			for _, alrt := range existingAlerts {
				if _, ok := activeFleetConnectorById[alrt.FunctionalityEntityId]; ok {
					alertsToDismiss = append(alertsToDismiss, alrt.Id)
				}
			}
		}
		if len(alertsToDismiss) > 0 {
			err = alertsManager.AutoResolveAlerts(alertsToDismiss)
			if err != nil {
				logger.GetLogger().Error("error while auto-resolving alerts for fleet", zap.Error(err))
				return err
			}
		}

		page++
	}
	return nil
}
func alertForFleetComponentsHealthCheck(ctx context.Context, alertsManager *alert.AlertsManager, tenantId string) error {
	db := config.GetDB()

	currentTime := time.Now()
	healthCheckTime := currentTime.Add(-time.Minute * time.Duration(FleetHealthCheckTimeNew))
	healthCheckIgnoreTime := currentTime.Add(-time.Minute * time.Duration(FleetHealthCheckIgnoreTimeNew))
	page, pageSize := 0, 50
	for {
		activeFleetComponent, inactiveFleetComponents, err := findInactiveAndActiveFleetComponents(db, tenantId, healthCheckTime, healthCheckIgnoreTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive fleet components", zap.Error(err))
			return err
		}
		logger.GetLoggerWithContext(ctx).Info("", zap.Any("Active Fleet Components", activeFleetComponent), zap.Any("Inactive Fleet Components", inactiveFleetComponents))
		if len(inactiveFleetComponents) == 0 && len(activeFleetComponent) == 0 {
			logger.GetLogger().Info("no more inactive fleet components found", zap.Int("page", page))
			break
		}
		err = sendFleetComponentInAppAlerts(inactiveFleetComponents, alertsManager)
		if err != nil {
			logger.GetLogger().Error("error while sending fleet component in-app alerts", zap.Error(err))
			return err
		}

		activeFleetComponentById := make(map[string]*fleet.Components)
		for _, af := range activeFleetComponent {
			activeFleetComponentById[af.Id.String()] = af
		}

		activeFleetComponentPartitions := util.PartitionSlice(activeFleetComponent, 10)
		var alertsToDismiss []string

		for _, activeFleetComponentPartition := range activeFleetComponentPartitions {
			fleetIds := make([]string, len(activeFleetComponentPartition))

			for i, a := range activeFleetComponentPartition {
				fleetIds[i] = a.Id.String()
			}

			existingAlerts, err := getExistingAlerts(ctx, tenantId, fleetIds, os.GetClient(), common.FleetNodeHealthCheck)
			if err != nil {
				logger.GetLogger().Error("error while fetching existing alerts for fleet components", zap.Error(err))
				return err
			}
			if len(existingAlerts) == 0 {
				logger.GetLogger().Error("alerts not found for fleet components", zap.Error(err))
				return err
			}

			for _, alert := range existingAlerts {
				if _, ok := activeFleetComponentById[alert.FunctionalityEntityId]; !ok {
					alertsToDismiss = append(alertsToDismiss, alert.Id)

				}
			}

		}
		if len(alertsToDismiss) > 0 {
			err = alertsManager.AutoResolveAlerts(alertsToDismiss)
			if err != nil {
				logger.GetLogger().Error("error while auto-resolving alerts", zap.Error(err))
				return err
			}
		}

		page++
	}
	return nil
}
func findActiveAndInactiveAgents(db *gorm.DB, tenantId string, healthCheckTime time.Time, page, pageSize int) ([]*agent.Agent, []*model.UnhealthyAgent, error) {
	var activeAgents []agent.Agent
	var inactiveAgents []agent.Agent
	offset := page * pageSize
	err := db.Where("tenant_id = ? AND heartbeat_at < ? AND status != ?", tenantId, healthCheckTime, StatusDeleted).
		Limit(pageSize).
		Offset(offset).
		Find(&inactiveAgents).Error

	if err != nil {
		return nil, nil, err
	}

	err = db.Where("tenant_id = ? AND heartbeat_at >= ? AND status != ?", tenantId, healthCheckTime, StatusDeleted).
		Limit(pageSize).
		Offset(offset).
		Find(&activeAgents).Error

	if err != nil {
		return nil, nil, err
	}

	if len(activeAgents) == 0 && len(inactiveAgents) == 0 {
		logger.GetLogger().Info("no active or inactive agents found", zap.Int("page", page))
		return nil, nil, nil
	}

	var activeResult []*agent.Agent
	for _, a := range activeAgents {
		activeResult = append(activeResult, &a)
	}

	healthCheckDuration := time.Since(healthCheckTime)
	var inactiveResult []*model.UnhealthyAgent
	for _, ag := range inactiveAgents {
		inactiveResult = append(inactiveResult, model.NewUnhealthyAgent(&ag, healthCheckDuration))
	}

	return activeResult, inactiveResult, err
}
func findActiveAndInactiveFleet(db *gorm.DB, tenantId string, healthCheckTime, healthCheckIgnoreTime time.Time, page, pageSize int) ([]*fleet.Node, []*model.UnhealthyFleet, error) {
	var ActiveFleetNodes []fleet.Node
	var InactiveFleetNodes []fleet.Node
	offSet := page * pageSize
	checkStatus := []string{healthchecker.StatusCreated, healthchecker.StatusInactive, healthchecker.StatusDisabled, healthchecker.StatusDeleted}

	err := db.Where("tenant_id = ? AND (heartbeat_at < ? AND heartbeat_at  > ?) AND status not in ?", tenantId, healthCheckTime, healthCheckIgnoreTime, checkStatus).
		Limit(pageSize).
		Offset(offSet).
		Find(&InactiveFleetNodes).Error

	if err != nil {
		return nil, nil, err
	}

	err = db.Where("tenant_id = ? AND (heartbeat_at >= ? AND heartbeat_at > ?) AND status not in ?", tenantId, healthCheckTime, healthCheckIgnoreTime, checkStatus).
		Limit(pageSize).
		Offset(offSet).
		Find(&ActiveFleetNodes).Error

	if err != nil {
		return nil, nil, err
	}

	if len(ActiveFleetNodes) == 0 && len(InactiveFleetNodes) == 0 {
		logger.GetLogger().Info("no active or inactive fleet nodes found", zap.Int("page", page))
		return nil, nil, nil

	}

	var activeResult []*fleet.Node
	for _, a := range ActiveFleetNodes {
		activeResult = append(activeResult, &a)
	}

	healthCheckDuration := time.Since(healthCheckTime)
	var inactiveResult []*model.UnhealthyFleet
	for _, fl := range InactiveFleetNodes {
		inactiveResult = append(inactiveResult, model.NewUnhealthyFleet(&fl, healthCheckDuration))
	}

	return activeResult, inactiveResult, err

}

func findActiveAndInactiveFleetConnectors(db *gorm.DB, tenantId string, healthCheckTime, healthCheckIgnoreTime time.Time, page, pageSize int) ([]*fleet.Connector, []*model.UnhealthyFleetConnector, error) {
	var activeFleetConnectors []fleet.Connector
	var inactiveFleetConnectors []fleet.Connector
	offset := page * pageSize
	checkStatus := []string{healthchecker.StatusCreated, healthchecker.StatusInactive, healthchecker.StatusDisabled, healthchecker.StatusDeleted}

	err := db.Where("tenant_id = ? AND (heartbeat_at < ? AND heartbeat_at > ? )AND status not in ?", tenantId, healthCheckTime, healthCheckIgnoreTime, checkStatus).
		Limit(pageSize).
		Offset(offset).
		Find(&inactiveFleetConnectors).Error

	if err != nil {
		return nil, nil, err
	}

	err = db.Where("tenant_id = ? AND (heartbeat_at >= ? AND heartbeat_at > ?) AND status not in ?", tenantId, healthCheckTime, healthCheckIgnoreTime, checkStatus).
		Limit(pageSize).
		Offset(offset).
		Find(&activeFleetConnectors).Error

	if err != nil {
		return nil, nil, err
	}

	if len(activeFleetConnectors) == 0 && len(inactiveFleetConnectors) == 0 {
		logger.GetLogger().Info("no active or inactive fleet connectors found", zap.Int("page", page))
		return nil, nil, nil
	}

	var activeResult []*fleet.Connector
	for _, ac := range activeFleetConnectors {
		activeResult = append(activeResult, &ac)
	}

	healthCheckDuration := time.Since(healthCheckTime)
	var inactiveResult []*model.UnhealthyFleetConnector
	for _, fc := range inactiveFleetConnectors {
		inactiveResult = append(inactiveResult, model.NewUnhealthyFleetConnector(&fc, healthCheckDuration))
	}

	return activeResult, inactiveResult, nil
}

func findInactiveAndActiveFleetComponents(db *gorm.DB, tenantId string, healthCheckTime, healthCheckIgnoreTime time.Time, page, pageSize int) ([]*fleet.Components, []*model.UnhealthyFleetComponents, error) {
	var activeFleetComponents []fleet.Components
	var inactiveFleetComponents []fleet.Components
	offset := page * pageSize
	checkStatus := []string{healthchecker.StatusCreated, healthchecker.StatusInactive, healthchecker.StatusDisabled, healthchecker.StatusDeleted}

	err := db.Where("tenant_id = ? AND (heartbeat_at < ? AND heartbeat_at > ?) AND status not in ?", tenantId, healthCheckTime, healthCheckIgnoreTime, checkStatus).
		Limit(pageSize).
		Offset(offset).
		Find(&inactiveFleetComponents).Error

	if err != nil {
		return nil, nil, err
	}

	err = db.Where("tenant_id = ? AND (heartbeat_at >= ? AND heartbeat_at > ?) AND status not in ?", tenantId, healthCheckTime, healthCheckIgnoreTime, checkStatus).
		Limit(pageSize).
		Offset(offset).
		Find(&activeFleetComponents).Error

	if err != nil {
		return nil, nil, err
	}

	if len(activeFleetComponents) == 0 && len(inactiveFleetComponents) == 0 {
		logger.GetLogger().Info("nothing found in db")
		return nil, nil, nil
	}

	healthCheckDuration := time.Since(healthCheckTime)
	var inactiveResult []*model.UnhealthyFleetComponents
	for _, fc := range inactiveFleetComponents {
		inactiveResult = append(inactiveResult, model.NewUnhealthyFleetComponents(&fc, healthCheckDuration))
	}

	var activeResult []*fleet.Components
	for _, fc := range activeFleetComponents {
		activeResult = append(activeResult, &fc)
	}

	return activeResult, inactiveResult, nil

}
func sendAgentInAppAlerts(inactiveAgents []*model.UnhealthyAgent, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(inactiveAgents))
	for i, ia := range inactiveAgents {
		newAlert, err := buildAgentAlert(*ia)
		if err != nil {
			logger.GetLogger().Error("error while building agent alert", zap.Error(err))
			return err
		}
		alertsToSave[i] = newAlert
	}
	return alertsManager.SendAlerts(alertsToSave)
}

func sendFleetInAppAlerts(inactiveFleet []*model.UnhealthyFleet, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(inactiveFleet))
	for i, uf := range inactiveFleet {
		newAlert, err := buildFleetAlert(*uf)
		if err != nil {
			logger.GetLogger().Error("error while building fleet alert", zap.Error(err))
			return err
		}
		alertsToSave[i] = newAlert
	}
	return alertsManager.SendAlerts(alertsToSave)
}
func sendFleetComponentInAppAlerts(inactiveFleetComponents []*model.UnhealthyFleetComponents, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(inactiveFleetComponents))
	for i, ufc := range inactiveFleetComponents {
		newAlert, err := buildFleetComponentAlert(*ufc)
		if err != nil {
			logger.GetLogger().Error("error while building fleet component alert", zap.Error(err))
			return err
		}
		alertsToSave[i] = newAlert
	}
	return alertsManager.SendAlerts(alertsToSave)
}

func sendFleetConnectorInAppAlerts(inactiveFleetConnectors []*model.UnhealthyFleetConnector, alertsManager *alert.AlertsManager) error {
	alertsToSave := make([]*alerts_async.Alert, len(inactiveFleetConnectors))
	for i, ufc := range inactiveFleetConnectors {
		newAlert, err := buildFleetConnectorAlert(*ufc)
		if err != nil {
			logger.GetLogger().Error("error while building fleet component alert", zap.Error(err))
			return err
		}
		alertsToSave[i] = newAlert
	}
	return alertsManager.SendAlerts(alertsToSave)
}

func buildAgentAlert(ia model.UnhealthyAgent) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.AgentHealthCheckerFunctionalityTitle, ia.HealthCheckTimeStr(), ia.GetEntityName())
	return alerts_async.NewAlert(
		alerts_async.Agent,
		alerts_async.WithEntity(ia),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.HealthCheck),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DHRW10001, ""),
	)
}

func buildFleetAlert(uf model.UnhealthyFleet) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.FleetNodeHealthCheckerFunctionalityTitle, uf.HealthCheckTimeStr(), uf.GetEntityName())
	return alerts_async.NewAlert(
		alerts_async.FleetNode,
		alerts_async.WithEntity(uf),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.HealthCheck),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DHRW10002, ""),
	)
}

func buildFleetConnectorAlert(uf model.UnhealthyFleetConnector) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.FleetConnectorHealthCheckerFunctionalityTitle, uf.HealthCheckTimeStr(), uf.GetEntityName())
	return alerts_async.NewAlert(
		alerts_async.FleetConnector,
		alerts_async.WithEntity(uf),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.HealthCheck),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DHRW10004, ""),
	)
}

func buildFleetComponentAlert(uf model.UnhealthyFleetComponents) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.FleetComponentHealthCheckTitle, uf.GetEntityName(), uf.HealthCheckTimeStr())
	return alerts_async.NewAlert(
		alerts_async.FleetComponent,
		alerts_async.WithEntity(uf),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.HealthCheck),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DHRW10003, ""),
	)
}

func getExistingAlerts(ctx context.Context, tenantId string, sourceIds []string, osClient *opensearch.Client, functionalityType string) ([]statistics.AlertDocument, error) {
	q := "tenantId:" + tenantId + " AND dismissed:false AND functionalityEntityId:" + "(" + strings.Join(sourceIds, " OR ") + ")" + ` AND functionalityType:(` + functionalityType + " OR " + alerts_async.IngestionChecker.String() + ")"
	openAlerts, err := os.Search(ctx, osClient, common.AlertsIndex, q)
	if err != nil {
		logger.GetLogger().Error("error while searching for alerts", zap.Error(err), zap.String("query", q))
		return nil, err
	}
	var alerts []statistics.AlertDocument
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
	if err != nil {
		logger.GetLogger().Error("error while creating decoder for alerts", zap.Error(err))
		return nil, err
	}
	err = decoder.Decode(openAlerts)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while decoding openSearch response", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}
	return alerts, nil
}
