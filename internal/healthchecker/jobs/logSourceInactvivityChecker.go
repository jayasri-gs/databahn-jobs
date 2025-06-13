package jobs

import (
	"context"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"

	"time"
)

const (
	DefaultLastSevenDays = 7 * 24 * time.Hour
)

func SendAlertsForInactivity(ctx context.Context) error {
	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Info("Failed to get tenants")
		return err
	}

	alertConfigMap, err := helper.EntityAlertConfigMapToTenantId(db)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Info("Failed to load alert config")
		return err
	}

	for _, t := range tenants {

		logger.GetLoggerWithContext(ctx).Info("processing alerts for tenant id ", zap.Any("tenantId", t.Id))

		tenantMap, ok := alertConfigMap[t.Id.String()]
		if !ok {
			continue
		}

		for interval, entityAlertsConfigs := range tenantMap {

			now := time.Now().UTC()
			var sourceIdsToAlert []string

			var activeSources []string

			for _, c := range entityAlertsConfigs {

				lastEventTime := c.LastCheckedTime

				if lastEventTime.Before(now.Add(-DefaultLastSevenDays)) {
					logger.GetLoggerWithContext(ctx).Info("Skipping source due to old last event time",
						zap.String("entityId", c.ID.String()),
						zap.Time("lastEventTime", lastEventTime))

					continue
				}

				if now.Sub(lastEventTime) > time.Duration(interval)*time.Minute {
					// Source is inactive, add to list for alerting
					sourceIdsToAlert = append(sourceIdsToAlert, c.EntityID.String())

				} else {
					// Source is active, add to list for potential alert dismissal
					activeSources = append(activeSources, c.EntityID.String())

				}

			}

			if len(sourceIdsToAlert) == 0 && len(activeSources) == 0 {
				logger.GetLoggerWithContext(ctx).Info("No sources to alert or dismiss for this interval.", zap.Any("interval", interval))
				continue
			}

			var validatedInactiveSources []source.Source
			statusCheck := []string{healthchecker.StatusCreated, healthchecker.StatusInactive, healthchecker.StatusDeleted, healthchecker.StatusDisabled}

			if len(sourceIdsToAlert) > 0 {
				err = db.Model(&source.Source{}).
					Where("status not in ? AND id in ?", statusCheck, sourceIdsToAlert).
					Find(&validatedInactiveSources).Error
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("Failed to get active sources from db for alerting",
						zap.Error(err), zap.Strings("sourceIds", sourceIdsToAlert))
					continue
				}
			}

			var alertsToSend []alerts_common.AlertBaseObjectV2
			for _, validSource := range validatedInactiveSources {
				alertsToSend = append(alertsToSend, alerts_common.AlertBaseObjectV2{
					EntityName:       validSource.Name,
					EntityId:         validSource.ID,
					EntityTenantUUId: validSource.TenantID,
					DataPlaneId:      validSource.DataPlaneId,
					AlertType:        alerts_common.AlertTypeExternalAndExternal,
					ErrorCode:        healthchecker.DNDW10001,
				})
			}

			if len(activeSources) > 0 {

				alerts, err := PaginatedOpenSearchCallToGetAllExistingAlerts(ctx, activeSources)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error getting existing alerts for active sources", zap.Error(err))
					return err
				}

				var toDismiss []alerts_common.AlertBaseObjectV2
				for _, alert := range alerts {
					var temp alerts_common.AlertBaseObjectV2
					temp.EntityName = alert.FunctionalityEntityName
					temp.EntityId = utils.UUIDFromStringOrNil(alert.FunctionalityEntityId)
					temp.EntityTenantUUId = utils.UUIDFromStringOrNil(alert.TenantId)
					temp.AlertType = alerts_common.AlertTypeExternalAndExternal
					temp.ErrorCode = healthchecker.DNDW10001
					toDismiss = append(toDismiss, temp)
				}

				logger.GetLoggerWithContext(ctx).Info("Dismissing Alerts for tenant Id", zap.Any("tenantId", t.Id))
				logger.GetLoggerWithContext(ctx).Info("dismissing alerts for sources ", zap.Any("sourceToDismiss", toDismiss))

				err = helper.SendAlertToControlPlane(ctx, toDismiss,
					fmt.Sprintf("No new events received in the last %d minutes", interval),
					fmt.Sprintf("No new events received in the last %d minutes", interval),
					alerts_common.LogSourceStatsNotReceived,
					alerts_common.LogSourceFunctionality,
					alerts_common.SevereAlert,
					alerts_common.AlertAutoResolved,
					true,
					"system")
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error while dismissing alerts for logSource activity check", zap.Error(err))
					return err
				}

			}

			if len(alertsToSend) > 0 {
				logger.GetLoggerWithContext(ctx).Info("Sending new alerts for tenant's inactive sources",
					zap.Any("sourcesToAlert", alertsToSend)) // Renamed from sourceToDismiss

				err = helper.SendAlertToControlPlane(ctx, alertsToSend,
					fmt.Sprintf("No new events received in the last %d minutes", interval),
					fmt.Sprintf("No new events received in the last %d minutes", interval),
					alerts_common.LogSourceStatsNotReceived,
					alerts_common.LogSourceFunctionality,
					alerts_common.SevereAlert,
					alerts_common.AlertOpen,
					false,
					"system",
				)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("Error while raising alert for log source activity check",
						zap.Error(err), zap.Any("sourcesToAlert", alertsToSend))
				}
			}

		}

	}

	return nil

}
