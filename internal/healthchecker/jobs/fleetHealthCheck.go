package jobs

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/db-models/fleet"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"time"
)

func fleetHealthChecker(ctx context.Context) error {
	currentTime := time.Now()
	healthCheckTime := currentTime.Add(-time.Minute * healthchecker.FleetHealthCheckTime)

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
	err = helper.SendAlertToControlFlag(ctx, fleetEntityArray, alerts_common.EdgeNodeHealthCheckTitle, alerts_common.EdgeNodeHealthCheckMessage, alerts_common.EdgeNodeHealthCheck, alerts_common.EdgeFunctionality, alerts_common.WarningAlert, alerts_common.AlertOpen, false, "system")
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while raising alert fleet inactivity", zap.Error(err))
		return err
	}
	return nil
}

//	func markLogSourceActive(ctx context.Context) error {
//		logging.GetLoggerWithContext(ctx).Info("processing log source activation status check")
//		aggObj, err := getAggStatsForLogSource(ctx, "", "")
//		if err != nil {
//			logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
//			return err
//		}
//		logging.GetLoggerWithContext(ctx).Info("pulled stats successfully", zap.Any("stats", aggObj))
//		var lsIdArray []string
//		for key, value := range aggObj.Agg {
//			valueInt, ok := value.(float64)
//			if !ok {
//				logging.GetLoggerWithContext(ctx).Error("error while getting value of stats", zap.Error(err))
//				return err
//			}
//			if valueInt > 0 {
//				_, err := uuid.Parse(key)
//				if err != nil {
//					continue
//				}
//				lsIdArray = append(lsIdArray, key)
//			}
//		}
//		if len(lsIdArray) > 0 {
//			logging.GetLoggerWithContext(ctx).Info("found log sources to activate", zap.Any("logSources", lsIdArray))
//			err := config.GetDB().Model(&logSource.LogSource{}).Where("id in ? and status in ?", lsIdArray, []int{constants.StatusAccepted, constants.StatusDeploying}).Updates(map[string]interface{}{"status": constants.StatusActive}).Error
//			if err != nil {
//				logging.GetLoggerWithContext(ctx).Error("error while updating status to active", zap.Error(err))
//				return err
//			}
//		} else {
//			logging.GetLoggerWithContext(ctx).Info("no found log sources to activate")
//		}
//		return nil
//	}
func HealthCheckAlertForFleetNode(ctx context.Context) error {
	defer logging.GetLogger().Sync()

	logging.GetLoggerWithContext(ctx).Debug("Handling alerts for appliances whose health reported is less than 15 minutes, Marking log source as active if it is reporting stats")

	err := fleetHealthChecker(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while handling alerts for unhealthy fleet nodes", zap.Error(err))
		return err
	}
	//err = markLogSourceActive(ctx)
	//if err != nil {
	//	logging.GetLoggerWithContext(ctx).Error("error while marking log sources as active", zap.Error(err))
	//	return err
	//}
	return nil
}
