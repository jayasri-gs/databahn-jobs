package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	notification_common "github.com/databahn-ai/common-utils/notification"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/notification"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
	"html/template"
	"strconv"
	"strings"
	"time"
)

// const emailTemplatesBasePath = "/home/databahn/templates/"
const emailTemplatesBasePath = "templates/"

func SendNotificationsForAlerts(ctx context.Context) error {
	db := config.GetDB()
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return err
	}
	osClient := os.GetClient()

	notificationManager, err := notification.NewNotificationManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating notification manager", zap.Error(err))
		return err
	}

	defer func() {
		notificationManager.Close(ctx)
	}()

	for _, t := range tenants {
		if t.Id.String() != "f5e31bb8-af80-40d8-a0e4-16f12187e4e4" {
			continue
		}
		checkpoint, err := entities.GetAlertNotificationCheckpoint(db, t.Id)
		tenantIdStr := t.Id.String()
		if err != nil {
			logger.GetLogger().Error("error while getting checkpoint", zap.Error(err), zap.String("tenant", tenantIdStr))
			continue
		}
		lastObservedAtFrom := time.Now().Add(-30 * time.Minute).UTC().UnixMilli()
		if checkpoint != nil {
			lastObservedAtFrom = checkpoint.CheckpointValue.LastObservedAt
			logger.GetLogger().Info("checkpoint found", zap.String("tenant", tenantIdStr), zap.Int64("lastObservedAtFrom", lastObservedAtFrom))
		}
		lastObservedAtTo := time.Now().Add(-2 * time.Minute).UTC().UnixMilli()
		q := "dismissed:false AND lastObservedAt:{" + strconv.FormatInt(lastObservedAtFrom, 10) + " TO " + strconv.FormatInt(lastObservedAtTo, 10) + "] AND tenantId:" + tenantIdStr
		pageSize := 200
		var after []any
		sort := []os.Sort{
			os.Sort{
				Field: "lastObservedAt",
				Order: "asc",
			},
		}
		targetsByModuleName, err := entities.GetTargetsForTenantByModule(db, t.Id)
		if err != nil {
			logger.GetLogger().Error("error while getting targets for tenant", zap.Error(err), zap.String("tenant", tenantIdStr))
			continue
		}

		var finalCheckpoint *entities.AlertNotificationCheckpoint
		if checkpoint == nil {
			finalCheckpoint = &entities.AlertNotificationCheckpoint{}
			finalCheckpoint.Id = uuid.New()
			finalCheckpoint.TenantId = t.Id
			finalCheckpoint.CheckpointValue = &entities.CheckpointValue{}
		} else {
			finalCheckpoint = checkpoint
		}
		var latestLastObserveAt int64 = 0
		alertsByFunctionalityAndTitle := make(map[string]map[string][]alerts_async.Alert)
		for {
			alertMap, newAfter, err := os.SearchPaginated(ctx, osClient, common.AlertsIndex, q, pageSize, after, sort)
			if err != nil {
				logger.GetLogger().Error("error while searching alerts", zap.Error(err), zap.String("tenant", tenantIdStr))
				break
			}
			var alerts []alerts_async.Alert
			decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &alerts})
			if err != nil {
				logger.GetLogger().Error("error while creating decoder for alerts", zap.Error(err))
				return err
			}
			err = decoder.Decode(alertMap)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("error while decoding openSearch response", zap.Error(err), zap.String("tenantId", tenantIdStr))
				return err
			}

			for _, alert := range alerts {
				if alertsByFunctionality, ok := alertsByFunctionalityAndTitle[alert.Functionality]; ok {
					if alertsByTitle, ok := alertsByFunctionality[alert.Title]; ok {
						alertsByTitle = append(alertsByTitle, alert)
						alertsByFunctionality[alert.Title] = alertsByTitle
					} else {
						alertsByFunctionality[alert.Title] = []alerts_async.Alert{alert}
					}
				} else {
					alertsByFunctionalityAndTitle[alert.Functionality] = map[string][]alerts_async.Alert{
						alert.Title: {alert},
					}
				}
				if alert.LastObservedAt > latestLastObserveAt {
					latestLastObserveAt = alert.LastObservedAt
				}
			}

			if len(alertMap) < pageSize {
				logger.GetLogger().Info("no more alerts to process", zap.String("tenant", tenantIdStr))
				break
			}

			after = newAfter
		}

		if len(targetsByModuleName) > 0 {
			for functionality, alertsByTitle := range alertsByFunctionalityAndTitle {
				for title, alerts := range alertsByTitle {
					for _, alert := range alerts {
						err := sendSupportNotification(alert, t, notificationManager)
						if err != nil {
							logger.GetLogger().Error("failed to send support notification", zap.Error(err), zap.String("tenant", tenantIdStr))
							return err
						} else {
							logger.GetLogger().Info("support notification sent successfully", zap.String("tenant", tenantIdStr), zap.String("functionality", functionality), zap.String("title", title))
						}
					}
					err := sendCustomerNotification(t, title, functionality, alerts, targetsByModuleName, notificationManager)
					if err != nil {
						logger.GetLogger().Error("failed to send customer notification", zap.Error(err), zap.String("tenant", tenantIdStr))
						return err
					} else {
						logger.GetLogger().Info("customer notification sent successfully for tenant", zap.String("tenant", tenantIdStr), zap.String("functionality", functionality), zap.String("title", title))
					}
				}
			}
		} else {
			logger.GetLogger().Info("no targets found for tenant", zap.String("tenant", tenantIdStr))
		}

		finalCheckpoint.CheckpointValue.LastObservedAt = latestLastObserveAt
		err = entities.UpdateAlertNotificationCheckpoint(db, finalCheckpoint)
		if err != nil {
			logger.GetLogger().Error("error while updating checkpoint", zap.Error(err), zap.String("tenant", tenantIdStr))
			return err
		} else {
			logger.GetLogger().Info("checkpoint updated successfully", zap.String("tenant", tenantIdStr), zap.Int64("lastObservedAt", latestLastObserveAt))
		}
	}
	return nil
}

func sendCustomerNotification(t tenant.Tenant, title, functionality string, alerts []alerts_async.Alert, targetsByModuleName map[string][]entities.Targets, notificationManager *notification.NotificationManager) error {
	subject := fmt.Sprintf("DataBahn.ai Alert - %s - %s", t.Name, title)
	body, err := buildEmailBody(title, alerts)
	if err != nil {
		logger.GetLogger().Error("error while building email body", zap.Error(err), zap.String("tenant", t.Id.String()))
		return err
	}
	for modulesName, targets := range targetsByModuleName {
		if alertFunctionalityMatchesModuleName(functionality, modulesName) {
			var databahnTargets []*notification_common.DatabahnTarget
			for _, target := range targets {
				databahnTargets = append(databahnTargets, &notification_common.DatabahnTarget{
					TenantId: t.Id.String(),
					TargetId: target.ID.String(),
				})
			}
			emailRequest := notification_common.EmailNotificationRequest{
				Targets: databahnTargets,
				Body:    body,
				Subject: subject,
			}
			err := notificationManager.SendEmailNotification(emailRequest)
			if err != nil {
				logger.GetLogger().Error("error while sending email notification", zap.Error(err), zap.String("tenant", t.Id.String()))
				return err
			}
		}
	}
	return nil
}

func sendSupportNotification(alert alerts_async.Alert, t tenant.Tenant, notificationManager *notification.NotificationManager) error {
	emailTitle := fmt.Sprintf("%s:%s:%s", alert.Functionality, t.Name, alert.FunctionalityEntityName)
	bodyBytes, err := json.MarshalIndent(alert, "", "  ")
	if err != nil {
		logger.GetLogger().Error("error while marshalling alert", zap.Error(err), zap.String("tenant", t.Id.String()))
		return err
	}
	body := string(bodyBytes)
	request := notification_common.OpsGenieNotificationRequest{
		Subject: emailTitle,
		Body:    body,
	}
	err = notificationManager.SendOpsGenieNotification(request)
	if err != nil {
		logger.GetLogger().Error("error while sending notification", zap.Error(err), zap.String("tenant", t.Id.String()))
		return err
	}
	return nil
}

func buildEmailBody(title string, alerts []alerts_async.Alert) (string, error) {
	var templatePath = emailTemplatesBasePath + "green_alert.html"
	switch alerts[0].Criticality {
	case alerts_async.Warning.String(), alerts_async.Sever.String():
		templatePath = emailTemplatesBasePath + "warning_alert.html"
	case alerts_async.Critical.String():
		templatePath = emailTemplatesBasePath + "error_alert.html"
	}
	t, err := template.ParseFiles(templatePath)
	if err != nil {
		logger.GetLogger().Error("error while parsing template", zap.Error(err))
		return "", err
	}
	buf := new(bytes.Buffer)
	var emailTemplateDetails []EmailTemplateDetails
	for _, alert := range alerts {
		emailTemplateDetails = append(emailTemplateDetails, EmailTemplateDetails{
			FunctionalityEntityName: alert.FunctionalityEntityName,
			FunctionalityType:       alert.FunctionalityType,
			Message:                 alert.Message,
			FirstObservedAt:         time.UnixMilli(alert.FirstObservedAt).Format(time.RFC3339),
		})
	}
	emailTemplate := EmailTemplate{
		Name:    "Dear Team,",
		Title:   title,
		Details: emailTemplateDetails,
	}
	err = t.Execute(buf, emailTemplate)
	if err != nil {
		logger.GetLogger().Error("error while executing template", zap.Error(err))
		return "", err
	}
	emailBody := buf.String()
	return emailBody, nil
}

func alertFunctionalityMatchesModuleName(functionality string, moduleName string) bool {
	return strings.EqualFold(functionality, moduleName)
}

type EmailTemplate struct {
	Name    string
	Title   string
	Details []EmailTemplateDetails
}

type EmailTemplateDetails struct {
	FunctionalityEntityName string
	FunctionalityType       string
	Message                 string
	FirstObservedAt         string
}
