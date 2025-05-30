package jobs

import (
	"context"
	"strconv"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/go-logging/logger"

	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"go.uber.org/zap"
)

func TenantDailyDigest(ctx context.Context) error {
	startTime := strconv.Itoa(int(time.Now().Add(-24 * time.Hour).UnixMilli()))
	endTime := strconv.Itoa(int(time.Now().UnixMilli()))

	tenants, err := tenant.GetTenants(ctx, config.GetDB())
	if err != nil {
		return err
	}

	logger.GetLogger().Info("daily digest for tenants", zap.String("startTime", startTime), zap.String("endTime", endTime), zap.Int("tenantCount", len(tenants)))

	alertsByTenant, err := tenant.GetAlertsFromOpenSearch(ctx)
	if err != nil {
		logger.GetLogger().Error("error while getting alerts from OpenSearch", zap.Error(err))
		return err
	}

	for _, t := range tenants {
		var tenantAlerts []statistics.AlertDocument

		if alerts, ok := alertsByTenant[t.Id.String()]; ok {
			tenantAlerts = alerts
		}

		logger.GetLogger().Info("processing tenant", zap.String("tenantId", t.Name))
		digest := tenant.GetDailyDigest(t.Id, t.Name, startTime, endTime, tenantAlerts)

		ingestedEventsByTenant, ingestedSizeByTenant, err := tenant.GetIngestionByTenantId(ctx, t.Id, startTime, endTime)
		if err != nil {
			logger.GetLogger().Error("error while getting ingestion by tenant id", zap.Error(err), zap.String("tenantId", t.Id.String()))
			continue
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
			continue
		}
		err = digest.GetIngestionBreakdown()
		if err != nil {
			logger.GetLogger().Error("error while setting ingestion breakdown", zap.Error(err))
			continue
		}
		digest.CalculateVolumeReductionAchievements()

		h := helper.Notification{
			TenantId:                digest.TenantId.String(),
			Subject:                 "Daily Digest - " + time.Now().Format(time.DateOnly),
			Message:                 digest,
			NotificationType:        "EMAIL",
			Suggestion:              "",
			AlertInfo:               "Daily Digest",
			Severity:                "info",
			Service:                 "DAILY_DIGEST",
			Granularity:             "tenant",
			Version:                 "v1",
			ID:                      digest.TenantId,
			Title:                   "Daily Digest - " + time.Now().Format(time.DateOnly),
			Functionality:           "DAILY_DIGEST",
			FunctionalityEntityId:   digest.TenantId.String(),
			FunctionalityEntityName: digest.Name,
			FunctionalityType:       "DAILY_DIGEST",
			FirstObservedAt:         time.Now(),
			LastObservedAt:          time.Now(),
		}
		err = helper.SendNotificationMessage(h)
		if err != nil {
			logger.GetLogger().Error("error while sending notification", zap.Error(err))
		}
		time.Sleep(5 * time.Second)
	}

	return nil
}
