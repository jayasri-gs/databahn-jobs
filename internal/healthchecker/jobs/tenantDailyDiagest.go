package jobs

import (
	"context"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/go-logging/logger"
	"strconv"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/config"
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

	// get total events ingested by tenant id
	ingestedEventsByTenant, ingestedSizeByTenant, err := tenant.GetIngestionByTenantId(ctx, startTime, endTime)
	if err != nil {
		logger.GetLogger().Error("error while getting total events ingested by tenant id", zap.Error(err))
		return err
	}

	for _, t := range tenants {
		logger.GetLogger().Info("processing tenant", zap.String("tenantId", t.Name))
		digest := tenant.GetDailyDigest(t.Id, t.Name, startTime, endTime)
		if err != nil {
			logger.GetLogger().Error("error while getting daily digest", zap.Error(err))
			continue
		}

		digest.GetIngestionStats(ingestedEventsByTenant[t.Id.String()], ingestedSizeByTenant[t.Id.String()])
		err = digest.GetSensitiveDataTrackingStats()
		if err != nil {
			logger.GetLogger().Error("error while setting sensitive data tracking stats", zap.Error(err))
		}
		digest.CalculateEPS()
		err := digest.GetEventDeliveryBreakdown()
		if err != nil {
			logger.GetLogger().Error("error while setting event delivery breakdown", zap.Error(err))
			continue
		}
		err = digest.GetIngestionBreakdown()
		if err != nil {
			logger.GetLogger().Error("error while setting ingestion breakdown", zap.Error(err))
			continue
		}
		digest.GetVolumeReductionAchievements()
		err = digest.GetAlerts(ctx, t.Id.String())
		if err != nil {
			logger.GetLogger().Error("error while getting alerts", zap.Error(err))
			continue
		}
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
