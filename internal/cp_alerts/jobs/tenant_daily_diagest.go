package jobs

import (
	"bytes"
	"context"
	notification_common "github.com/databahn-ai/common-utils/notification"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/notification"
	"strconv"
	"text/template"
	"time"

	"github.com/databahn-ai/go-logging/logger"

	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"go.uber.org/zap"
)

const DailyDigestModule = "DAILY_DIGEST"

func SendTenantDailyDigest(ctx context.Context) error {
	startTime := strconv.Itoa(int(time.Now().Add(-24 * time.Hour).UnixMilli()))
	endTime := strconv.Itoa(int(time.Now().UnixMilli()))

	db := config.GetDB()
	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		return err
	}

	logger.GetLogger().Info("daily digest for tenants", zap.String("startTime", startTime), zap.String("endTime", endTime), zap.Int("tenantCount", len(tenants)))

	alertsByTenant, err := tenant.GetAlertsFromOpenSearch(ctx)
	if err != nil {
		logger.GetLogger().Error("error while getting alerts from OpenSearch", zap.Error(err))
		return err
	}

	notificationManager, err := notification.NewNotificationManager(ctx)
	if err != nil {
		logger.GetLogger().Error("error while creating notification manager", zap.Error(err))
		return err
	}

	for _, t := range tenants {
		targets, err := entities.GetTargetsForModule(db, t.Id, DailyDigestModule)
		if err != nil {
			logger.GetLogger().Error("error while getting targets for tenant", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}
		if len(targets) == 0 {
			logger.GetLogger().Info("no targets found for tenant, skipping", zap.String("tenantId", t.Id.String()))
			continue
		}

		digest, err := buildDigest(ctx, alertsByTenant, t, startTime, endTime)
		if err != nil {
			logger.GetLogger().Error("failed to build daily digest for tenant", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}
		err = sendNotification(digest, targets, t, notificationManager)
		if err != nil {
			logger.GetLogger().Error("failed to send notification for tenant", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
		}
	}
	notificationManager.Close(ctx)
	return nil
}

func sendNotification(digest *tenant.Digest, targets []entities.Targets, t tenant.Tenant, notificationManager *notification.NotificationManager) error {
	templatePath := EmailTemplatesBasePath + "daily_digest.html"
	temp, err := template.ParseFiles(templatePath)
	if err != nil {
		logger.GetLogger().Error("error while parsing template", zap.Error(err))
		return err
	}
	buf := new(bytes.Buffer)
	err = temp.Execute(buf, digest)
	if err != nil {
		logger.GetLogger().Error("error while executing template", zap.Error(err))
		return err
	}
	emailBody := buf.String()
	subject := "Daily Digest - " + time.Now().Format(time.DateOnly)
	var databahnTargets []*notification_common.DatabahnTarget
	for _, target := range targets {
		databahnTargets = append(databahnTargets, &notification_common.DatabahnTarget{
			TenantId: t.Id.String(),
			TargetId: target.ID.String(),
		})
	}
	emailRequest := notification_common.EmailNotificationRequest{
		Targets: databahnTargets,
		Body:    emailBody,
		Subject: subject,
	}
	err = notificationManager.SendEmailNotification(emailRequest)
	if err != nil {
		logger.GetLogger().Error("error while sending email notification", zap.Error(err), zap.String("tenant", t.Id.String()))
		return err
	}
	return nil
}

func buildDigest(ctx context.Context, alertsByTenant map[string][]statistics.AlertDocument, t tenant.Tenant, startTime string, endTime string) (*tenant.Digest, error) {
	var tenantAlerts []statistics.AlertDocument
	if alerts, ok := alertsByTenant[t.Id.String()]; ok {
		tenantAlerts = alerts
	}

	logger.GetLogger().Info("processing tenant", zap.String("tenantId", t.Name))
	digest := tenant.GetDailyDigest(t.Id, t.Name, startTime, endTime, tenantAlerts)

	ingestedEventsByTenant, ingestedSizeByTenant, err := tenant.GetIngestionByTenantId(ctx, t.Id, startTime, endTime)
	if err != nil {
		logger.GetLogger().Error("error while getting ingestion by tenant id", zap.Error(err), zap.String("tenantId", t.Id.String()))
		return nil, err
	}
	digest.CalculateIngestionStats(ingestedEventsByTenant, ingestedSizeByTenant)
	err = digest.GetSensitiveDataTrackingStats()
	if err != nil {
		logger.GetLogger().Error("error while setting sensitive data tracking stats", zap.Error(err))
	}
	digest.CalculateEPS()
	err = digest.GetEventDeliveryBreakdown()
	if err != nil {
		logger.GetLogger().Error("error while setting event delivery breakdown", zap.Error(err))
		return nil, err
	}
	err = digest.GetIngestionBreakdown()
	if err != nil {
		logger.GetLogger().Error("error while setting ingestion breakdown", zap.Error(err))
		return nil, err
	}
	digest.CalculateVolumeReductionAchievements()
	return digest, err
}
