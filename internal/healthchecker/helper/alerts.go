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

func SendAlertToControlFlag(ctx context.Context, entityArray []alerts_common.AlertEntityObject, title string, message string, functionalityType string, functionality string, severity string) error {
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
			Dismissed:               false,
			DismissedBy:             "",
			Criticality:             severity,
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

