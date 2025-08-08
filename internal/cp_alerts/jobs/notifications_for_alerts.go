package jobs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"strconv"
	"strings"
	"text/template"
	"time"

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
	"go.uber.org/zap"
)

const EmailTemplatesBasePath = "/home/databahn/templates/"

// const EmailTemplatesBasePath = "templates/"

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

	// Load module tenant configs once for all tenants
	tenantToModuleToConfigMap, err := entities.LoadTenantToModuleToConfigs(db)
	if err != nil {
		logger.GetLogger().Error("error while loading module tenant configs", zap.Error(err))
		return err
	}

	for _, t := range tenants {
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
		logger.GetLogger().Info("checking alerts between", zap.Int64("from", lastObservedAtFrom),
			zap.String("tenant", tenantIdStr), zap.Int64("to", lastObservedAtTo))
		q := "dismissed:false AND lastObservedAt:{" + strconv.FormatInt(lastObservedAtFrom, 10) + " TO " + strconv.FormatInt(lastObservedAtTo, 10) + "] AND tenantId:" + tenantIdStr
		pageSize := 200
		var after []any
		sort := []os.Sort{
			os.Sort{
				Field: "lastObservedAt",
				Order: "asc",
			},
			os.Sort{
				Field: "id",
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
		var latestLastObserveAt int64 = finalCheckpoint.CheckpointValue.LastObservedAt
		alertsByFunctionalityAndFunctionalityType := make(map[string]map[string][]alerts_async.Alert)
		for {
			alertMap, newAfter, err := os.SearchPaginated(ctx, osClient, common.AlertsIndex, q, pageSize, after, sort)
			if err != nil {
				logger.GetLogger().Error("error while searching alerts", zap.Error(err), zap.String("tenant", tenantIdStr))
				break
			}
			var alerts []alerts_async.Alert
			decoder, err := util.CreateAlertDecoder(&alerts)
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
				if alertsByFunctionality, ok := alertsByFunctionalityAndFunctionalityType[alert.Functionality]; ok {
					if alertsByFunctionalityType, ok := alertsByFunctionality[alert.FunctionalityType]; ok {
						alertsByFunctionalityType = append(alertsByFunctionalityType, alert)
						alertsByFunctionality[alert.FunctionalityType] = alertsByFunctionalityType
					} else {
						alertsByFunctionality[alert.FunctionalityType] = []alerts_async.Alert{alert}
					}
				} else {
					alertsByFunctionalityAndFunctionalityType[alert.Functionality] = map[string][]alerts_async.Alert{
						alert.FunctionalityType: {alert},
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

		for functionality, alertsByFunctionalityType := range alertsByFunctionalityAndFunctionalityType {
			for functionalityType, alerts := range alertsByFunctionalityType {
				for _, alert := range alerts {
					err := sendSupportNotification(alert, t, notificationManager)
					if err != nil {
						logger.GetLogger().Error("failed to send support notification", zap.Error(err), zap.String("tenant", tenantIdStr))
						return err
					} else {
						logger.GetLogger().Info("support notification sent successfully", zap.String("tenant", tenantIdStr), zap.String("functionality", functionality), zap.String("title", alert.Title))
					}
				}
				if len(targetsByModuleName) > 0 {
					err := sendCustomerNotification(t, functionalityType, functionality, alerts, targetsByModuleName, notificationManager, tenantToModuleToConfigMap)
					if err != nil {
						logger.GetLogger().Error("failed to send customer notification", zap.Error(err), zap.String("tenant", tenantIdStr))
						return err
					} else {
						logger.GetLogger().Info("customer notification sent successfully for tenant", zap.String("tenant", tenantIdStr), zap.String("functionality", functionality),
							zap.String("functionalityType", functionalityType), zap.Int("alertsCount", len(alerts)))
					}
				} else {
					logger.GetLogger().Info("no targets found for tenant, no customer alerts", zap.String("tenant", tenantIdStr))
				}
			}
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

func sendCustomerNotification(t tenant.Tenant, functionalityType, functionality string, alerts []alerts_async.Alert, targetsByModuleName map[string][]entities.Targets, notificationManager *notification.NotificationManager, tenantToModuleToConfigMap map[string]map[string]*entities.ModuleTenantConfigData) error {
	for modulesName, targets := range targetsByModuleName {
		if alertFunctionalityMatchesModuleName(functionality, modulesName) {
			var databahnTargets []*notification_common.DatabahnTarget
			for _, target := range targets {
				databahnTargets = append(databahnTargets, &notification_common.DatabahnTarget{
					TenantId: t.Id.String(),
					TargetId: target.ID.String(),
				})
			}
			var filteredAlerts []alerts_async.Alert
			if alerts_async.LogSource.String() == functionality || alerts_async.CloudLogSource.String() == functionality {
				var configFound *entities.ModuleTenantConfigData
				moduleConfigMap := tenantToModuleToConfigMap[t.Id.String()]
				if moduleConfigMap != nil {
					cfg := moduleConfigMap[modulesName]
					if cfg != nil {
						configFound = cfg
						logger.GetLogger().Info("module config found for tenant", zap.String("tenant", t.Id.String()), zap.String("module", modulesName))
					}
				}
				if configFound != nil && configFound.SourceList.IncludeExclude == "EXCLUDE" {
					// Create a map of source IDs for efficient lookup
					sourceIdMap := make(map[string]bool)
					for _, sourceId := range configFound.SourceList.SourceIds {
						sourceIdMap[sourceId] = true
					}

					// Filter alerts using the map
					for _, alert := range alerts {
						if !sourceIdMap[alert.FunctionalityEntityId] {
							filteredAlerts = append(filteredAlerts, alert)
						}
					}
				} else {
					filteredAlerts = alerts
				}
			} else {
				filteredAlerts = alerts
			}

			// If no alerts remain after filtering, don't send notification
			if len(filteredAlerts) == 0 {
				logger.GetLogger().Info("no alerts remaining after filtering, skipping notification",
					zap.String("tenant", t.Id.String()),
					zap.String("functionality", functionality))
				continue
			}

			emailTitle := buildEmailTitle(functionalityType)

			subject := fmt.Sprintf("DataBahn.ai Alert - %s - %s", t.Name, emailTitle)

			// Rebuild email body with filtered alerts
			body, err := buildEmailBody(emailTitle, filteredAlerts)
			if err != nil {
				logger.GetLogger().Error("error while building email body with filtered alerts", zap.Error(err), zap.String("tenant", t.Id.String()))
				return err
			}
			emailRequest := notification_common.EmailNotificationRequest{
				Targets: databahnTargets,
				Body:    body,
				Subject: subject,
			}
			err = notificationManager.SendEmailNotification(emailRequest)
			if err != nil {
				logger.GetLogger().Error("error while sending email notification", zap.Error(err), zap.String("tenant", t.Id.String()))
				return err
			}
		}
	}
	return nil
}

func sendSupportNotification(alert alerts_async.Alert, t tenant.Tenant, notificationManager *notification.NotificationManager) error {
	emailTitle := fmt.Sprintf("%s:%s:%s", alert.FunctionalityType, t.Name, alert.FunctionalityEntityName)
	body, err := buildOpsGenieBody(t, alert)
	if err != nil {
		logger.GetLogger().Error("error while building opsgenie body", zap.Error(err), zap.String("tenant", t.Id.String()))
		return err
	}
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

func buildOpsGenieBody(tnt tenant.Tenant, alert alerts_async.Alert) (string, error) {
	var templatePath = EmailTemplatesBasePath + "operations_alert.html"
	t, err := template.ParseFiles(templatePath)
	if err != nil {
		logger.GetLogger().Error("error while parsing template", zap.Error(err))
		return "", err
	}
	buf := new(bytes.Buffer)
	emailTemplate := OpsGenieDetails{
		TenantName: tnt.Name,
		Alert:      alert,
	}
	err = t.Execute(buf, emailTemplate)
	if err != nil {
		logger.GetLogger().Error("error while executing template", zap.Error(err))
		return "", err
	}
	emailBody := buf.String()
	return emailBody, nil
}

func buildEmailTitle(functionalityType string) string {
	titleMap := make(map[string]string)
	titleMap[alerts_async.IngestionChecker.String()] = "No new data ingested"
	titleMap[alerts_async.DeliveryChecker.String()] = "No data delivered"
	if title, exists := titleMap[functionalityType]; exists {
		return title
	}
	title := strings.ReplaceAll(functionalityType, "_", " ")
	title = strings.ReplaceAll(title, "-", " ")
	return title
}

func buildEmailBody(emailTitle string, alerts []alerts_async.Alert) (string, error) {
	var templatePath = EmailTemplatesBasePath + "green_alert.html"
	switch alerts[0].Criticality {
	case alerts_async.Warning.String(), alerts_async.Sever.String():
		templatePath = EmailTemplatesBasePath + "warning_alert.html"
	case alerts_async.Critical.String():
		templatePath = EmailTemplatesBasePath + "error_alert.html"
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
			Title:                   alert.Title,
			FirstObservedAt:         time.UnixMilli(alert.FirstObservedAt).Format(time.RFC3339),
		})
	}
	emailTemplate := EmailTemplate{
		Name:    "Dear Team,",
		Title:   emailTitle,
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
	if strings.EqualFold("fleet", moduleName) {
		return strings.HasPrefix(strings.ToLower(functionality), "fleet")
	}
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
	Title                   string
}

type OpsGenieDetails struct {
	TenantName string
	Alert      alerts_async.Alert
}
