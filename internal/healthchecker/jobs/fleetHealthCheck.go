package jobs

import (
	"context"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/agent"
	"github.com/databahn-ai/databahn-jobs/internal/store/fleet"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func agentHealthChecker(ctx context.Context) error {
	currentTime := time.Now()
	healthCheckTime := currentTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.AgentHealthCheckTime)))
	logging.GetLogger().Info("checking agent health")
	var agents []agent.Agent
	err := config.GetDB().Find(&agents, "heartbeat_at < ? AND status != ?", healthCheckTime, common.StatusCreated).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Error in running get unhealthy agent query", zap.Error(err))
		return err
	}
	if len(agents) <= 0 {
		logging.GetLoggerWithContext(ctx).Info("no unhealthy agent found")
		return nil
	}
	//creating fleet alert object for fleets
	var agentAlerts []alerts_common.AlertBaseObjectV2
	for _, ed := range agents {
		var temp alerts_common.AlertBaseObjectV2
		temp.EntityName = ed.Name
		temp.EntityId = ed.ID
		temp.EntityTenantUUId = ed.TenantId
		temp.DataPlaneId = ed.DataPlaneId
		temp.AlertType = alerts_common.AlertTypeExternalAndExternal
		temp.ErrorCode = healthchecker.DNDE10003
		agentAlerts = append(agentAlerts, temp)
	}

	// raise alert and save it to opensearch
	err = helper.SendAlertToControlPlane(ctx, agentAlerts, common.AgentHealthCheckTitle, common.AgentHealthCheckMessage,
		common.AgentHealthCheck, common.AgentFunctionality, alerts_common.SevereAlert,
		alerts_common.AlertOpen, false, "system")
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while raising alert agent inactivity", zap.Error(err))
		return err
	}
	logging.GetLogger().Info("alert raised successfully for unhealthy agent nodes", zap.Int("no_of_agent", len(agentAlerts)))
	return nil
}

func fleetHealthChecker(ctx context.Context) error {
	currentTime := time.Now()
	healthCheckTime := currentTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckTime)))
	healthCheckIgnoreTime := currentTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckIgnoreTime)))
	logging.GetLogger().Info("checking fleet health")
	//getting fleet nodes having heartbeat less than 15 minutes
	var fleetNodes []fleet.Node
	checkStatus := []string{healthchecker.StatusDisabled, healthchecker.StatusDeleted, healthchecker.StatusCreated, healthchecker.StatusInactive}
	err := config.GetDB().Where("(heartbeat_at < ? AND heartbeat_at > ?) AND status not in ?", healthCheckTime, healthCheckIgnoreTime, checkStatus).Debug().Find(&fleetNodes).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Error in running get unhealthy fleet node query", zap.Error(err))
		return err
	}
	if len(fleetNodes) <= 0 {
		logging.GetLoggerWithContext(ctx).Info("no unhealthy fleet node found", zap.Any("fleets", fleetNodes))
		return err
	}

	logging.GetLogger().Info("found unhealthy fleet nodes", zap.Any("no_unhealthy_nodes", len(fleetNodes)))

	//creating fleet alert object for fleets
	var fleetEntityArray []alerts_common.AlertBaseObjectV2
	for _, ed := range fleetNodes {
		var temp alerts_common.AlertBaseObjectV2
		temp.EntityName = ed.Name
		temp.EntityId = ed.Id
		temp.EntityTenantUUId = ed.TenantId
		temp.AlertType = alerts_common.AlertTypeExternalAndExternal
		temp.ErrorCode = healthchecker.DNDE10003
		fleetEntityArray = append(fleetEntityArray, temp)
	}

	// raise alert and save it to opensearch
	err = helper.SendAlertToControlPlane(ctx, fleetEntityArray, common.FleetNodeHealthCheckTitle, common.FleetNodeHealthCheckMessage, alerts_common.FleetNodeHealthCheck, alerts_common.FleetFunctionality, alerts_common.WarningAlert, alerts_common.AlertOpen, false, "system")
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while raising alert fleet inactivity", zap.Error(err))
		return err
	}
	logging.GetLogger().Info("alert raised successfully for unhealthy fleet nodes", zap.Int("no_of_fleets", len(fleetNodes)))
	return nil
}

func connectorHealthChecker(ctx context.Context) error {
	currentTime := time.Now()
	healthCheckTime := currentTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckTime)))
	healthCheckIgnoreTime := currentTime.Add(-time.Minute * time.Duration(util.GetEnvInt64FromString(healthchecker.FleetHealthCheckIgnoreTime)))
	logging.GetLogger().Info("checking fleet connector health")

	var connectors []fleet.Connector
	checkStatus := []string{healthchecker.StatusDisabled, healthchecker.StatusDeleted, healthchecker.StatusCreated, healthchecker.StatusInactive}
	err := config.GetDB().Find(&connectors, "(heartbeat_at < ? AND heartbeat_at > ?) AND status not in ?", healthCheckTime, healthCheckIgnoreTime, checkStatus).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Error in running get unhealthy fleet connector query", zap.Error(err))
		return err
	}
	if len(connectors) <= 0 {
		logging.GetLoggerWithContext(ctx).Info("no unhealthy fleet Connector found")
		return err
	}

	logging.GetLogger().Info("found unhealthy fleet connector", zap.Any("no_unhealthy_nodes", len(connectors)))

	var connectorEntity []alerts_common.AlertBaseObjectV2
	for _, ed := range connectors {
		var temp alerts_common.AlertBaseObjectV2
		temp.EntityName = ed.Name
		temp.EntityId = ed.ID
		temp.EntityTenantUUId = ed.TenantID
		temp.AlertType = alerts_common.AlertTypeExternalAndExternal
		temp.ErrorCode = healthchecker.DNDE10003
		connectorEntity = append(connectorEntity, temp)
	}

	// raise alert and save it to opensearch
	err = helper.SendAlertToControlPlane(ctx, connectorEntity, common.FleetConnectorHealthCheckTitle, common.FleetConnectorHealthCheckMessage, alerts_common.FleetConnectorHealthCheck, alerts_common.FleetFunctionality, alerts_common.WarningAlert, alerts_common.AlertOpen, false, "system")
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while raising alert connector inactivity", zap.Error(err))
		return err
	}
	logging.GetLogger().Info("alert raised successfully for unhealthy fleet connector", zap.Int("no_of_unhealthy_connectors", len(connectorEntity)))
	return nil
}

func HealthCheckAlertForFleetNode(ctx context.Context) error {
	defer logging.GetLogger().Sync()

	logging.GetLoggerWithContext(ctx).Debug("Handling alerts for appliances whose health reported is less than 15 minutes, Marking log source as active if it is reporting stats")

	err := fleetHealthChecker(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while handling alerts for unhealthy fleet nodes", zap.Error(err))
		return err
	}
	err = connectorHealthChecker(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while handling alerts for unhealthy connectors", zap.Error(err))
		return err
	}

	err = agentHealthChecker(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while handling alerts for unhealthy agents", zap.Error(err))
		return err
	}
	return nil
}
