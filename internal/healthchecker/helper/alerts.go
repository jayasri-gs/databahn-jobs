package helper

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/databahn-ai/common-utils/alert"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/db-models/alerts_common"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"time"
)

func SendAlertToControlPlane(ctx context.Context, entityArray []alerts_common.AlertBaseObjectV2, title string, message string, functionalityType string, functionality string, severity string, status int, dismissed bool, updatedBy string) error {
	var alerts []alerts_common.Alert
	for _, entity := range entityArray {
		eMsg := configuration.GetErrorMessage(context.Background(), config.GetAlertConfiguration(), entity.ErrorCode, message)
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
			AlertType:               entity.AlertType,
			DataPlaneId:             entity.DataPlaneId,
			ErrorMessage:            eMsg,
			ErrorCode:               entity.ErrorCode,
		}
		alerts = append(alerts, temp)
	}
	return sendAlertToCP(ctx, alerts, !dismissed)
}

func sendAlertToCP(ctx context.Context, alerts []alerts_common.Alert, sendNotification bool) error {
	alertClient, err := alert.NewAlertClient(ctx, config.GetAppConfiguration())
	if err != nil {
		logger.GetLogger().Error("error while creating alert client", zap.Error(err))
		return err
	}
	logger.GetLoggerWithContext(ctx).Info("sending alert to CP", zap.Any("alerts", alerts))
	if sendNotification {
		err = SendToNotificationTopic(ctx, alerts)
		if err != nil {
			logger.GetLogger().Error("Enable to Send Alert to Notification Topic ", zap.Error(err))
		}
		logger.GetLogger().Info("Notification alert sent successfully")
	}
	resp, err1 := alertClient.SendAlertWithRetry(alert.Request{Alerts: alerts}, 3)
	if err1 != nil {
		return err
	}
	logger.GetLogger().Info("alert sent successfully", zap.Any("response", resp))

	return nil
}
func SendToNotificationTopic(ctx context.Context, alerts []alerts_common.Alert) error {
	logger.GetLoggerWithContext(ctx).Info("sending alert to notification topic", zap.Any("alerts", alerts))
	producer := GetProducer()

	if producer == nil {
		logger.GetLogger().Error("error while getting producer")
		return errors.New("error while getting producer")
	}

	for _, alt := range alerts {
		if alt.Criticality == "severe" || alt.Criticality == "critical" {
			alt.Criticality = "critical"
		} else if alt.Criticality == "warning" {
			alt.Criticality = "warning"
		} else {
			alt.Criticality = "info"
		}

		notification := Notification{
			TenantId:                alt.TenantUUID.String(),
			Subject:                 alt.Title,
			Message:                 alt.Message,
			NotificationType:        "EMAIL",
			Suggestion:              "",
			AlertInfo:               alt.Title,
			Severity:                alt.Criticality,
			Service:                 alt.Functionality,
			Granularity:             "tenant",
			Version:                 "v1",
			ID:                      alt.ID,
			Title:                   alt.Title,
			Functionality:           alt.Functionality,
			FunctionalityEntityId:   alt.FunctionalityEntityId,
			FunctionalityEntityName: alt.FunctionalityEntityName,
			FunctionalityType:       alt.FunctionalityType,
			FirstObservedAt:         alt.FirstObservedAt,
			LastObservedAt:          alt.LastObservedAt,
		}
		nfbyts, er := json.Marshal(notification)
		if er != nil {
			logger.GetLogger().Error("error while marshalling notification", zap.Error(er), zap.Reflect("notification", notification))
			continue
		}

		headers := make([]kafka.Header, 1)
		headers[0] = kafka.Header{Key: "notification", Value: nfbyts}
		message := kafka.Message{
			Message: nfbyts,
			Headers: headers,
		}

		//ADDED CHANGES FOR DYNAMIC TOPIC
		producer.SendAsyncTopic(message, healthchecker.NotificationTopic, func(err error) {
			logger.GetLogger().Error("Error while sending alt", zap.Error(err), zap.Reflect("Notification", notification))
		})
		logger.GetLogger().Info("alert sent successfully", zap.Any("response", notification))
	}
	count := producer.Producer.Flush(1000)
	logger.GetLogger().Info("Producer Flush  complete.", zap.Int("count", count))
	return nil
}

func SendNotificationMessage(notification Notification) error {
	producer := GetProducer()

	if producer == nil {
		logger.GetLogger().Error("error while getting producer")
		return errors.New("error while getting producer")
	}

	nfbyts, er := json.Marshal(notification)
	if er != nil {
		logger.GetLogger().Error("error while marshalling notification", zap.Error(er), zap.Reflect("notification", notification))
		return er
	}
	message := kafka.Message{
		Message: nfbyts,
	}

	//ADDED CHANGES FOR DYNAMIC TOPIC
	producer.SendAsyncTopic(message, healthchecker.NotificationTopic, func(err error) {
		logger.GetLogger().Error("Error while sending alt", zap.Error(err), zap.Reflect("Notification", notification))
	})
	logger.GetLogger().Info("alert sent successfully", zap.Any("response", notification))
	count := producer.Producer.Flush(1000)
	logger.GetLogger().Info("Producer Flush  complete.", zap.Int("count", count))
	return nil
}
