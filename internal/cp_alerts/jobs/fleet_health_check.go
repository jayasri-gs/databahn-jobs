package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"github.com/databahn-ai/databahn-jobs/internal/store/fleet"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func HealthCheckJob(ctx context.Context) error {

	logger.GetLoggerWithContext(ctx).Info("starting health check job")

	logger.GetLoggerWithContext(ctx).Info("Starting health check for unhealthy agents")
	err := alertForUnhealthyAgents(ctx)
	if err != nil {
		logger.GetLogger().Error("error while handling alerts for unhealthy agents", zap.Error(err))
		return err
	}

	logger.GetLoggerWithContext(ctx).Info("Starting health check for unhealthy fleet nodes")
	err = alertForFleetHealthCheck(ctx)
	if err != nil {
		logger.GetLogger().Error("error while handling alerts for unhealthy fleet nodes", zap.Error(err))
		return err
	}

	logger.GetLoggerWithContext(ctx).Info("Starting health check for unhealthy fleet connectors")
	err = alertForFleetConnectorsHealthCheck(ctx)
	if err != nil {
		logger.GetLogger().Error("error while handling alerts for unhealthy fleet connectors", zap.Error(err))
		return err
	}

	logger.GetLoggerWithContext(ctx).Info("Starting health check for unhealthy fleet components")
	err = alertForFleetComponentsHealthCheck(ctx)
	if err != nil {
		logger.GetLogger().Error("error while handling alerts for unhealthy fleet components", zap.Error(err))
		return err
	}

	return nil
}

func alertForUnhealthyAgents(ctx context.Context) error {
	db := config.GetDB()
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return err
	}
	defer alertsManager.Close(ctx)

	healthCheckTime := time.Now().Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.AgentHealthCheckTime)))
	page, pageSize := 0, 50
	for {
		inactiveAgents, err := findInactiveAgents(db, healthCheckTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive agents", zap.Error(err))
			return err
		}
		if len(inactiveAgents) == 0 {
			logger.GetLogger().Info("no more inactive agents found", zap.Int("page", page))
		} else {
			err := sendAgentInAppAlerts(inactiveAgents, alertsManager)
			if err != nil {
				logger.GetLogger().Error("error while sending agent in-app alerts", zap.Error(err))
				return err
			}
		}
		page++
	}
	return nil
}

func alertForFleetHealthCheck(ctx context.Context) error {
	db := config.GetDB()
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return err
	}
	defer alertsManager.Close(ctx)

	healthCheckTime := time.Now().Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckTime)))
	healthCheckIgnoreTime := time.Now().Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckIgnoreTime)))
	page, pageSize := 0, 50
	for {
		inactiveFleet, err := findInactiveFleet(db, healthCheckTime, healthCheckIgnoreTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive fleet nodes", zap.Error(err))
			return err
		}
		if len(inactiveFleet) == 0 {
			logger.GetLogger().Info("no more inactive fleet nodes found", zap.Int("page", page))
			break
		} else {
			err := sendFleetInAppAlerts(inactiveFleet, alertsManager)
			if err != nil {
				logger.GetLogger().Error("error while sending fleet in-app alerts", zap.Error(err))
				return err
			}
		}
		page++
	}
	return nil
}

func alertForFleetConnectorsHealthCheck(ctx context.Context) error {
	db := config.GetDB()
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return err
	}
	defer alertsManager.Close(ctx)

	healthCheckTime := time.Now().Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckTime)))
	healthCheckIgnoreTime := time.Now().Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckIgnoreTime)))
	page, pageSize := 0, 50
	for {
		inactiveFleetConnectors, err := findInactiveFleetConnectors(db, healthCheckTime, healthCheckIgnoreTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive fleet connectors", zap.Error(err))
			return err
		}
		if len(inactiveFleetConnectors) == 0 {
			logger.GetLogger().Info("no more inactive fleet connectors found", zap.Int("page", page))
			break
		} else {
			for _, fc := range inactiveFleetConnectors {
				newAlert, err := buildFleetConnectorAlert(*fc)
				if err != nil {
					logger.GetLogger().Error("error while building fleet connector alert", zap.Error(err))
					return err
				}
				err = alertsManager.SendAlerts([]*alerts_async.Alert{newAlert})
				if err != nil {
					logger.GetLogger().Error("error while sending fleet connector in-app alerts", zap.Error(err))
					return err
				}
			}
		}
		page++
	}
	return nil
}
func alertForFleetComponentsHealthCheck(ctx context.Context) error {
	db := config.GetDB()
	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating alerts manager", zap.Error(err))
		return err
	}
	defer alertsManager.Close(ctx)
	currentTime := time.Now()
	healthCheckTime := currentTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckTime)))
	healthCheckIgnoreTime := currentTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckIgnoreTime)))
	page, pageSize := 0, 50
	for {
		inactiveFleetComponents, err := findInactiveFleetComponents(db, healthCheckTime, healthCheckIgnoreTime, page, pageSize)
		if err != nil {
			logger.GetLogger().Error("error while finding inactive fleet components", zap.Error(err))
			return err
		}
		if len(inactiveFleetComponents) == 0 {
			logger.GetLogger().Info("no more inactive fleet components found", zap.Int("page", page))
			break
		} else {
			err := sendFleetComponentInAppAlerts(inactiveFleetComponents, alertsManager)
			if err != nil {
				logger.GetLogger().Error("error while sending fleet component in-app alerts", zap.Error(err))
				return err
			}
		}
		page++
	}
	return nil
}
func findInactiveAgents(db *gorm.DB, healthCheckTime time.Time, page, pageSize int) ([]*model.UnhealthyAgent, error) {
	var agents []agent.Agent
	offset := page * pageSize
	err := db.Table("agents").
		Where("heartbeat_at < ? AND status != ?", healthCheckTime, healthchecker.StatusCreated).
		Limit(pageSize).
		Offset(offset).
		Find(&agents).Error
	if err != nil {
		return nil, err
	}
	if len(agents) == 0 {
		logger.GetLogger().Info("no inactive agents found", zap.Int("page", page))
		return nil, nil
	}
	var result []*model.UnhealthyAgent
	for _, a := range agents {
		result = append(result, &model.UnhealthyAgent{
			Agent: &a,
		})
	}
	return result, nil
}
func findInactiveFleet(db *gorm.DB, healthCheckTime, healthCheckIgnoreTime time.Time, page, pageSize int) ([]*model.UnhealthyFleet, error) {
	var fleetNodes []fleet.Node
	offSet := page * pageSize
	checkStatus := []string{healthchecker.StatusCreated, healthchecker.StatusInactive, healthchecker.StatusDisabled, healthchecker.StatusDeleted}
	err := db.Table("fleet_nodes").
		Where("(heartbeat_at < ? AND heartbeat_at  > ?) AND status not in ?", healthCheckTime, healthCheckIgnoreTime, checkStatus).
		Limit(pageSize).
		Offset(offSet).
		Find(&fleetNodes).Error

	if err != nil {
		return nil, err
	}
	if len(fleetNodes) == 0 {
		logger.GetLogger().Info("no inactive fleet nodes found", zap.Int("page", page))
		return nil, nil
	}
	var result []*model.UnhealthyFleet
	for _, fl := range fleetNodes {
		result = append(result, &model.UnhealthyFleet{
			FleetNode: &fl,
		})
	}
	return result, nil

}

func findInactiveFleetConnectors(db *gorm.DB, healthCheckTime, healthCheckIgnoreTime time.Time, page, pageSize int) ([]*model.UnhealthyFleetConnector, error) {
	var fleetConnectors []fleet.Connector
	offset := page * pageSize
	checkStatus := []string{healthchecker.StatusCreated, healthchecker.StatusInactive, healthchecker.StatusDisabled, healthchecker.StatusDeleted}
	err := db.Table("connector").
		Where("(heartbeat_at < ? AND heartbeat_at > ? )AND status not in ?", healthCheckTime, healthCheckIgnoreTime, checkStatus).
		Limit(pageSize).
		Offset(offset).
		Find(&fleetConnectors).Error
	if err != nil {
		return nil, err
	}
	if len(fleetConnectors) == 0 {
		logger.GetLogger().Info("no inactive fleet connectors found", zap.Int("page", page))
		return nil, nil
	}
	var result []*model.UnhealthyFleetConnector
	for _, fc := range fleetConnectors {
		result = append(result, &model.UnhealthyFleetConnector{
			FleetConnector: &fc,
		})
	}
	return result, nil
}

func findInactiveFleetComponents(db *gorm.DB, healthCheckTime, healthCheckIgnoreTime time.Time, page, pageSize int) ([]*model.UnhealthyFleetComponents, error) {
	var fleetComponents []fleet.Components
	offset := page * pageSize
	checkStatus := []string{healthchecker.StatusCreated, healthchecker.StatusInactive, healthchecker.StatusDisabled, healthchecker.StatusDeleted}
	err := db.Table("fleet_components").
		Where("(heartbeat_at < ? AND heartbeat_at > ?) AND status not in ?", healthCheckTime, checkStatus).
		Limit(pageSize).
		Offset(offset).
		Find(&fleetComponents).Error
	if err != nil {
		return nil, err
	}
	if len(fleetComponents) == 0 {
		logger.GetLogger().Info("no inactive fleet components found", zap.Int("page", page))
		return nil, nil
	}
	var result []*model.UnhealthyFleetComponents
	for _, fc := range fleetComponents {
		result = append(result, &model.UnhealthyFleetComponents{
			FleetComponent: &fc,
		})
	}
	return result, nil
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

func buildAgentAlert(ia model.UnhealthyAgent) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.AgentHealthCheckerFunctionalityTitle, ia.GetEntityName())
	return alerts_async.NewAlert(
		alerts_async.CloudLogSource,
		alerts_async.WithEntity(ia),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.IngestionChecker),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DNDW10001, ""),
	)
}

func buildFleetAlert(uf model.UnhealthyFleet) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.FleetNodeHealthCheckerFunctionalityTitle, uf.GetEntityName())
	return alerts_async.NewAlert(
		alerts_async.CloudLogSource,
		alerts_async.WithEntity(uf),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.IngestionChecker),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DNDW10001, ""),
	)
}

func buildFleetConnectorAlert(uf model.UnhealthyFleetConnector) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.FleetConnectorHealthCheckerFunctionalityTitle, uf.GetEntityName())
	return alerts_async.NewAlert(
		alerts_async.CloudLogSource,
		alerts_async.WithEntity(uf),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.IngestionChecker),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DNDW10001, ""),
	)
}

func buildFleetComponentAlert(uf model.UnhealthyFleetComponents) (*alerts_async.Alert, error) {
	details := fmt.Sprintf(common.FleetComponentHealthCheckTitle, uf.GetEntityName())
	return alerts_async.NewAlert(
		alerts_async.CloudLogSource,
		alerts_async.WithEntity(uf),
		alerts_async.WithCriticality(alerts_async.Critical),
		alerts_async.WithFunctionalityType(alerts_async.IngestionChecker),
		alerts_async.WithTitle(details),
		alerts_async.WithMessage(details),
		alerts_async.WithErrorCode(alerts_async.DNDW10001, ""),
	)
}
