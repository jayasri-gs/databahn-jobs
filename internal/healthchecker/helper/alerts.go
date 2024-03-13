package helper

import (
	"context"
	"github.com/databahn-ai/common-utils/alert"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"time"
)

func SendDismissALertsToControlFlag(ctx context.Context, toDismissAlerts []alerts_common.AlertEntityObject) error {
	var alerts []alerts_common.Alert
	for _, entity := range toDismissAlerts {
		temp := alerts_common.Alert{
			FunctionalityEntityId: entity.EntityId.String(),
			Dismissed:             true,
			Status:                alerts_common.AlertAutoResolved,
			UpdatedAt:             time.Now(),
			UpdatedBy:             "system",
		}
		alerts = append(alerts, temp)
	}

	alertClient, err := alert.NewAlertClient(ctx, config.GetAppConfiguration())
	if err != nil {
		logger.GetLogger().Error("error while creating alert client", zap.Error(err))
		return err
	}
	resp, err := alertClient.SendAlertWithRetry(alert.Request{Alerts: alerts}, 3)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("alert dismissed successfully", zap.Any("response", resp))

	return nil
}
func SendAlertToControlFlag(ctx context.Context, entityArray []alerts_common.AlertEntityObject, title string, message string, functionalityType string, functionality string, severity string, status int, dismissed bool, updatedBy string) error {
	var alerts []alerts_common.Alert
	for _, entity := range entityArray {
		temp := alerts_common.Alert{
			Title:                   title,
			Message:                 message,
			CreatedAt:               time.Now(),
			UpdatedAt:               time.Now(),
			FirstObservedAt:         time.Now(),
			LastObservedAt:          time.Now(),
			TenantUUID:              entity.EntityTenantUUId,
			FunctionalityType:       functionalityType,
			Functionality:           functionality,
			FunctionalityEntityId:   entity.EntityId.String(),
			FunctionalityEntityName: entity.EntityName,
			Dismissed:               dismissed,
			Criticality:             severity,
			Status:                  status,
			UpdatedBy:               updatedBy,
		}
		alerts = append(alerts, temp)
	}

	alertClient, err := alert.NewAlertClient(ctx, config.GetAppConfiguration())
	if err != nil {
		logger.GetLogger().Error("error while creating alert client", zap.Error(err))
		return err
	}
	resp, err := alertClient.SendAlertWithRetry(alert.Request{Alerts: alerts}, 3)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("alert sent successfully", zap.Any("response", resp))

	return nil
}
