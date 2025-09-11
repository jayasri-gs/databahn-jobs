package jobs

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/opensearch-project/opensearch-go/v2"
	"gorm.io/gorm"

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

// FunctionalitiesRequiringAggregation defines the list of functionalities that require alert aggregation
var FunctionalitiesRequiringAggregation = []string{
	"agent",
}

func SendNotificationsForAlerts(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	db := config.GetDB()
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		errorMsg := fmt.Sprintf("error while getting tenants: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while getting tenants", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}
	osClient := os.GetClient()

	notificationManager, err := notification.NewNotificationManager(ctx)
	if err != nil {
		errorMsg := fmt.Sprintf("error while creating notification manager: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while creating notification manager", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	defer func() {
		notificationManager.Close(ctx)
	}()

	// Load module tenant configs once for all tenants
	tenantToModuleToConfigMap, err := entities.LoadTenantToModuleToConfigs(db)
	if err != nil {
		errorMsg := fmt.Sprintf("error while loading module tenant configs: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while loading module tenant configs", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	for _, t := range tenants {
		tenantId := t.Id.String()
		err := processExternalAlerts(ctx, db, t, osClient, notificationManager, tenantToModuleToConfigMap)
		if err != nil {
			errorMsg := fmt.Sprintf("failed process external alerts for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("failed process external alerts", zap.Error(err), zap.String("tenant", tenantId))
		}
	}
	t := tenant.Tenant{
		Id:   uuid.MustParse(common.DatabahnTenantId),
		Name: "Databahn Engineering",
	}
	tenants = append(tenants, t)
	for _, t := range tenants {
		tenantId := t.Id.String()
		err := processInternalAlerts(ctx, db, t, osClient, notificationManager)
		if err != nil {
			errorMsg := fmt.Sprintf("failed process internal alerts for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("failed process internal alerts", zap.Error(err), zap.String("tenant", tenantId))
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("successfully completed notifications for alerts")
		return common.NewJobResultSuccess()
	} else {
		logger.GetLogger().Info("notifications for alerts completed with errors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}

func processExternalAlerts(ctx context.Context, db *gorm.DB, t tenant.Tenant, osClient *opensearch.Client, notificationManager *notification.NotificationManager, tenantToModuleToConfigMap map[string]map[string]*entities.ModuleTenantConfigData) error {
	checkpoint, err := entities.GetAlertNotificationCheckpoint(db, t.Id, alerts_async.External)
	tenantIdStr := t.Id.String()
	if err != nil {
		logger.GetLogger().Error("error while getting checkpoint", zap.Error(err), zap.String("tenant", tenantIdStr))
		return err
	}
	lastObservedAtFrom := time.Now().Add(-30 * time.Minute).UTC().UnixMilli()
	if checkpoint != nil {
		lastObservedAtFrom = checkpoint.CheckpointValue.LastObservedAt
		logger.GetLogger().Info("checkpoint found", zap.String("tenant", tenantIdStr), zap.Int64("lastObservedAtFrom", lastObservedAtFrom))
	}

	targetsByModuleName, err := entities.GetTargetsForTenantByModule(db, t.Id)
	if err != nil {
		logger.GetLogger().Error("error while getting targets for tenant", zap.Error(err), zap.String("tenant", tenantIdStr))
		return err
	}

	var finalCheckpoint *entities.AlertNotificationCheckpoint
	if checkpoint == nil {
		finalCheckpoint = &entities.AlertNotificationCheckpoint{}
		finalCheckpoint.Id = uuid.New()
		finalCheckpoint.TenantId = t.Id
		finalCheckpoint.CheckpointValue = &entities.CheckpointValue{}
		finalCheckpoint.AlertType = alerts_async.External.String()
	} else {
		finalCheckpoint = checkpoint
	}

	alertsByFunctionalityAndFunctionalityType, latestLastObserveAt, err := readAlertsSinceCheckpoint(ctx, common.AlertsIndex, lastObservedAtFrom, tenantIdStr, osClient, finalCheckpoint.CheckpointValue.LastObservedAt)
	if err != nil {
		return err
	}
	for functionality, alertsByFunctionalityType := range alertsByFunctionalityAndFunctionalityType {
		// Check if functionality requires aggregation
		if aggregationRequired(functionality) {
			aggregatedAlerts := aggregateAlertbyfunctionalityType(functionality, alertsByFunctionalityType)
			// Replace the original alerts with the aggregated alerts
			alertsByFunctionalityType = aggregatedAlerts
		}
		for functionalityType, alerts := range alertsByFunctionalityType {
			for _, alert := range alerts {
				err := sendSupportNotification(alert, t, notificationManager, false)
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
	return nil
}

func processInternalAlerts(ctx context.Context, db *gorm.DB, t tenant.Tenant, osClient *opensearch.Client, notificationManager *notification.NotificationManager) error {
	checkpoint, err := entities.GetAlertNotificationCheckpoint(db, t.Id, alerts_async.Internal)
	tenantIdStr := t.Id.String()
	if err != nil {
		logger.GetLogger().Error("error while getting checkpoint", zap.Error(err), zap.String("tenant", tenantIdStr))
		return err
	}
	lastObservedAtFrom := time.Now().Add(-30 * time.Minute).UTC().UnixMilli()
	if checkpoint != nil {
		lastObservedAtFrom = checkpoint.CheckpointValue.LastObservedAt
		logger.GetLogger().Info("checkpoint found", zap.String("tenant", tenantIdStr), zap.Int64("lastObservedAtFrom", lastObservedAtFrom))
	}

	var finalCheckpoint *entities.AlertNotificationCheckpoint
	if checkpoint == nil {
		finalCheckpoint = &entities.AlertNotificationCheckpoint{}
		finalCheckpoint.Id = uuid.New()
		finalCheckpoint.TenantId = t.Id
		finalCheckpoint.CheckpointValue = &entities.CheckpointValue{}
		finalCheckpoint.AlertType = alerts_async.Internal.String()
	} else {
		finalCheckpoint = checkpoint
	}

	alertsByFunctionalityAndFunctionalityType, latestLastObserveAt, err := readAlertsSinceCheckpoint(ctx, common.AlertsIndexInternal, lastObservedAtFrom, tenantIdStr, osClient, finalCheckpoint.CheckpointValue.LastObservedAt)
	if err != nil {
		return err
	}
	for functionality, alertsByFunctionalityType := range alertsByFunctionalityAndFunctionalityType {
		// Check if functionality requires aggregation
		if aggregationRequired(functionality) {
			aggregatedAlerts := aggregateAlertbyfunctionalityType(functionality, alertsByFunctionalityType)
			// Replace the original alerts with the aggregated alerts
			alertsByFunctionalityType = aggregatedAlerts
		}
		for functionalityType, alerts := range alertsByFunctionalityType {
			for _, alert := range alerts {
				err := sendSupportNotification(alert, t, notificationManager, true)
				if err != nil {
					logger.GetLogger().Error("failed to send support notification", zap.Error(err), zap.String("tenant", tenantIdStr))
					return err
				} else {
					logger.GetLogger().Info("support notification sent successfully", zap.String("tenant", tenantIdStr), zap.String("functionality", functionality), zap.String("functionalityType", functionalityType), zap.String("title", alert.Title))
				}
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
	return nil
}

func readAlertsSinceCheckpoint(ctx context.Context, indexName string, lastObservedAtFrom int64, tenantIdStr string, osClient *opensearch.Client, latestLastObserveAt int64) (map[string]map[string][]alerts_async.Alert, int64, error) {
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

	alertsByFunctionalityAndFunctionalityType := make(map[string]map[string][]alerts_async.Alert)
	for {
		alertMap, newAfter, err := os.SearchPaginated(ctx, osClient, indexName, q, pageSize, after, sort)
		if err != nil {
			logger.GetLogger().Error("error while searching alerts", zap.Error(err), zap.String("tenant", tenantIdStr))
			break
		}
		var alerts []alerts_async.Alert
		decoder, err := util.CreateAlertDecoder(&alerts)
		if err != nil {
			logger.GetLogger().Error("error while creating decoder for alerts", zap.Error(err))
			return nil, 0, err
		}
		err = decoder.Decode(alertMap)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("error while decoding openSearch response", zap.Error(err), zap.String("tenantId", tenantIdStr))
			return nil, 0, err
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
	return alertsByFunctionalityAndFunctionalityType, latestLastObserveAt, nil
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

func sendSupportNotification(alert alerts_async.Alert, t tenant.Tenant, notificationManager *notification.NotificationManager, isInternal bool) error {
	emailTitle := fmt.Sprintf("%s:%s:%s", alert.FunctionalityType, t.Name, alert.FunctionalityEntityName)
	if isInternal {
		emailTitle += ":internal"
	}
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
			Message:                 strings.ReplaceAll(alert.Message, "\n", "<br>"),
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

var prefixMatchModules = []string{"fleet", "volume_control"}

func alertFunctionalityMatchesModuleName(functionality string, moduleName string) bool {
	exactMatch := strings.EqualFold(functionality, moduleName)
	if !exactMatch {
		for _, prefix := range prefixMatchModules {
			if strings.EqualFold(prefix, moduleName) {
				return strings.HasPrefix(strings.ToLower(functionality), prefix)
			}
		}
	}
	return exactMatch
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

// aggregationRequired checks if the given functionality requires alert aggregation
func aggregationRequired(functionality string) bool {
	for _, f := range FunctionalitiesRequiringAggregation {
		if f == functionality {
			return true
		}
	}
	return false
}

// aggregateAlertbyfunctionalityType aggregates alerts by functionalityType for agent functionality
// Returns a map where each functionalityType contains a single aggregated alert
func aggregateAlertbyfunctionalityType(functionality string, alertsByFunctionalityType map[string][]alerts_async.Alert) map[string][]alerts_async.Alert {
	aggregatedAlerts := make(map[string][]alerts_async.Alert)

	for functionalityType, alerts := range alertsByFunctionalityType {
		if len(alerts) == 0 {
			continue
		}

		// Use the first alert as a base for the aggregated alert
		aggregatedAlert := alerts[0]

		// Aggregate the title to show it's a combined alert
		aggregatedAlert.Title = fmt.Sprintf("Aggregated %s Alert (%d Entities)", functionality, len(alerts))

		// Aggregate messages from all alerts
		var messages []string
		var entityNames []string

		for _, alert := range alerts {
			messages = append(messages, fmt.Sprintf("%s: %s", alert.FunctionalityEntityName, alert.Message))
			entityNames = append(entityNames, alert.FunctionalityEntityName)
		}

		// Combine all messages
		aggregatedAlert.Message = strings.Join(messages, "\n")

		// Update the functionality entity name to show multiple entities
		if len(entityNames) > 1 {
			aggregatedAlert.FunctionalityEntityName = fmt.Sprintf("Multiple Entities (%d)", len(entityNames))
		}

		// Use the latest LastObservedAt from all alerts
		for _, alert := range alerts {
			if alert.LastObservedAt > aggregatedAlert.LastObservedAt {
				aggregatedAlert.LastObservedAt = alert.LastObservedAt
			}
		}

		// Store the single aggregated alert
		aggregatedAlerts[functionalityType] = []alerts_async.Alert{aggregatedAlert}
	}

	return aggregatedAlerts
}
