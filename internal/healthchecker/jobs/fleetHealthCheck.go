package jobs

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/db-models/fleet"
	logging "github.com/databahn-ai/go-logging/logger"

	"go.uber.org/zap"
	"time"
)

func HealthCheckAlertForFleetNode(ctx context.Context) error {
	currentTime := time.Now()
	healthCheckTime := currentTime.Add(-time.Minute * 15)

	//getting fleet nodes having heartbeat less than 15 minutes
	var fleetNodes []fleet.FleetNode
	err := config.GetDB().Find(&fleetNodes, "heartbeat_at < ? AND status != 0", healthCheckTime).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("Error in running get unhealthy fleet node query", zap.Error(err))
		return err
	}
	if len(fleetNodes) <= 0 {
		logging.GetLoggerWithContext(ctx).Info("no unhealthy fleet node found", zap.Any("fleets", fleetNodes))
		return err
	}

	//creating fleet alert object for fleets
	var fleetEntityArray []alerts_common.AlertEntityObject
	for _, ed := range fleetNodes {
		var temp alerts_common.AlertEntityObject
		temp.EntityName = ed.Name
		temp.EntityId = ed.Id
		temp.EntityTenantUUId = ed.TenantUUID
		fleetEntityArray = append(fleetEntityArray, temp)
	}

	// raise alert and save it to opensearch
	err = helper.SaveAlertToOpenSearch(ctx, fleetEntityArray, alerts_common.EdgeNodeHealthCheckTitle, alerts_common.EdgeNodeHealthCheckMessage, alerts_common.EdgeNodeHealthCheck, alerts_common.EdgeFunctionality, alerts_common.WarningAlert)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while raising alert fleet inactivity", zap.Error(err))
		return err
	}
	return nil
}
