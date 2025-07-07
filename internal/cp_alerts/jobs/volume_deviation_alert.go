package jobs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/model"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func SendAlertForVolumeDeviation(ctx context.Context) error {
	db := config.GetDB()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error getting all tenants", zap.Error(err))
		return err
	}

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error getting alerts manager", zap.Error(err))
		return err
	}

	defer func() {
		alertsManager.Close(ctx)
	}()

	for _, t := range tenants {
		tenantIdUuid := t.Id
		tenantId := t.Id.String()

		logger.GetLoggerWithContext(ctx).Info("checking volume deviation for tenant", zap.String("tenant_id", tenantId))

		// Calculate time ranges
		now := time.Now().UTC()
		todayStart := now.Add(-24 * time.Hour)
		todayEnd := now
		yesterdayStart := now.Add(-48 * time.Hour)
		yesterdayEnd := now.Add(-24 * time.Hour)

		// Get today's stats
		todayIngestion, todayDelivery, err := getTenantStats(ctx, tenantIdUuid, todayStart, todayEnd)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("Error getting today's stats", zap.Error(err), zap.String("tenant_id", tenantId))
			continue
		}

		// Get yesterday's stats
		yesterdayIngestion, yesterdayDelivery, err := getTenantStats(ctx, tenantIdUuid, yesterdayStart, yesterdayEnd)
		if err != nil {
			logger.GetLoggerWithContext(ctx).Error("Error getting yesterday's stats", zap.Error(err), zap.String("tenant_id", tenantId))
			continue
		}

		// Calculate deviations
		ingestionDeviation := calculateDeviation(todayIngestion, yesterdayIngestion)
		deliveryDeviation := calculateDeviation(todayDelivery, yesterdayDelivery)

		// Check if below 50% threshold
		isIngestionBelow50 := todayIngestion < (yesterdayIngestion * 0.5)
		isDeliveryBelow50 := todayDelivery < (yesterdayDelivery * 0.5)

		// Check if above 150% threshold (spike)
		isIngestionAbove150 := todayIngestion > (yesterdayIngestion * 1.5)
		isDeliveryAbove150 := todayDelivery > (yesterdayDelivery * 1.5)

		// Create separate alerts for ingestion and delivery drops/spikes
		var alertsToSend []*alerts_async.Alert

		// Alert for ingestion drop
		if isIngestionBelow50 {
			ingestionDropAlert := model.NewVolumeDeviationAlert(
				&t,
				todayIngestion,
				todayDelivery,
				yesterdayIngestion,
				yesterdayDelivery,
				ingestionDeviation,
				deliveryDeviation,
				true,
				false,
				model.VolumeDeviationBelow50Percent,
			)

			alert, err := buildVolumeDeviationAlert(*ingestionDropAlert)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error building ingestion drop alert", zap.Error(err), zap.String("tenant_id", tenantId))
			} else {
				alertsToSend = append(alertsToSend, alert)
			}
		}

		// Alert for ingestion spike
		if isIngestionAbove150 {
			ingestionSpikeAlert := model.NewVolumeDeviationAlert(
				&t,
				todayIngestion,
				todayDelivery,
				yesterdayIngestion,
				yesterdayDelivery,
				ingestionDeviation,
				deliveryDeviation,
				false,
				true,
				model.VolumeDeviationAboveStdDev,
			)

			alert, err := buildVolumeDeviationAlert(*ingestionSpikeAlert)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error building ingestion spike alert", zap.Error(err), zap.String("tenant_id", tenantId))
			} else {
				alertsToSend = append(alertsToSend, alert)
			}
		}

		// Alert for delivery drop
		if isDeliveryBelow50 {
			deliveryDropAlert := model.NewVolumeDeviationAlert(
				&t,
				todayIngestion,
				todayDelivery,
				yesterdayIngestion,
				yesterdayDelivery,
				ingestionDeviation,
				deliveryDeviation,
				true,
				false,
				model.VolumeDeviationBelow50Percent,
			)

			alert, err := buildVolumeDeviationAlert(*deliveryDropAlert)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error building delivery drop alert", zap.Error(err), zap.String("tenant_id", tenantId))
			} else {
				alertsToSend = append(alertsToSend, alert)
			}
		}

		// Alert for delivery spike
		if isDeliveryAbove150 {
			deliverySpikeAlert := model.NewVolumeDeviationAlert(
				&t,
				todayIngestion,
				todayDelivery,
				yesterdayIngestion,
				yesterdayDelivery,
				ingestionDeviation,
				deliveryDeviation,
				false,
				true,
				model.VolumeDeviationAboveStdDev,
			)

			alert, err := buildVolumeDeviationAlert(*deliverySpikeAlert)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error building delivery spike alert", zap.Error(err), zap.String("tenant_id", tenantId))
			} else {
				alertsToSend = append(alertsToSend, alert)
			}
		}

		// Send all alerts
		if len(alertsToSend) > 0 {
			err = alertsManager.SendAlerts(alertsToSend)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("Error sending volume deviation alerts", zap.Error(err), zap.String("tenant_id", tenantId))
			} else {
				logger.GetLoggerWithContext(ctx).Info("Successfully sent volume deviation alerts",
					zap.String("tenant_id", tenantId),
					zap.Int("alerts_sent", len(alertsToSend)))
			}
		}

		logger.GetLoggerWithContext(ctx).Info("completed volume deviation check for tenant",
			zap.String("tenant_id", tenantId),
			zap.Float64("today_ingestion", todayIngestion),
			zap.Float64("yesterday_ingestion", yesterdayIngestion),
			zap.Float64("today_delivery", todayDelivery),
			zap.Float64("yesterday_delivery", yesterdayDelivery),
			zap.Bool("below_50_percent", isIngestionBelow50 || isDeliveryBelow50),
			zap.Bool("above_150_percent", isIngestionAbove150 || isDeliveryAbove150),
			zap.Int("total_alerts_generated", len(alertsToSend)))
	}

	return nil
}

// getTenantStats gets ingestion and delivery stats for a tenant in the given time range
func getTenantStats(ctx context.Context, tenantId uuid.UUID, startTime, endTime time.Time) (float64, float64, error) {
	startTimeStr := strconv.FormatInt(startTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(endTime.UnixMilli(), 10)

	// Get ingestion stats
	ingestionQuery := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	ingestionResponse, err := statistics.GetStatsSum(ctx, ingestionQuery, tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, 0, err
	}

	// Get delivery stats
	deliveryQuery := `tags.component_name: "dispenser" AND name: "total_events_delivered"`
	deliveryResponse, err := statistics.GetStatsSum(ctx, deliveryQuery, tenantId, startTimeStr, endTimeStr)
	if err != nil {
		return 0, 0, err
	}

	return ingestionResponse.Sum, deliveryResponse.Sum, nil
}

// calculateDeviation calculates the percentage deviation between two values
func calculateDeviation(current, previous float64) float64 {
	if previous == 0 {
		if current == 0 {
			return 0
		}
		return 100 // 100% increase from 0
	}
	return ((current - previous) / previous) * 100
}

// buildVolumeDeviationAlert builds an alert for volume deviation using the proper architecture
func buildVolumeDeviationAlert(vda model.VolumeDeviationAlert) (*alerts_async.Alert, error) {
	var title, message string
	var functionality alerts_async.Functionality
	var errorCode alerts_async.ErrorCode

	// Get date strings for better context
	todayDate := time.Now().UTC().Format("January 2, 2006")
	yesterdayDate := time.Now().UTC().Add(-24 * time.Hour).Format("January 2, 2006")

	if vda.IsBelow50Percent {
		if vda.TodayIngestion < (vda.YesterdayIngestion * 0.5) {
			title = fmt.Sprintf("Volume Drop Alert: Ingestion volume dropped by %.1f%%", -vda.IngestionDeviation)
			message = fmt.Sprintf("Ingestion volume dropped from %.0f events (%s) to %.0f events (%s) - %.1f%% decrease",
				vda.YesterdayIngestion, yesterdayDate, vda.TodayIngestion, todayDate, -vda.IngestionDeviation)
			functionality = alerts_async.LogSource
			errorCode = alerts_async.DGRW10001
		} else {
			title = fmt.Sprintf("Volume Drop Alert: Delivery volume dropped by %.1f%%", -vda.DeliveryDeviation)
			message = fmt.Sprintf("Delivery volume dropped from %.0f events (%s) to %.0f events (%s) - %.1f%% decrease",
				vda.YesterdayDelivery, yesterdayDate, vda.TodayDelivery, todayDate, -vda.DeliveryDeviation)
			functionality = alerts_async.Dispenser
			errorCode = alerts_async.DGRW10001
		}
	} else {
		if vda.TodayIngestion > (vda.YesterdayIngestion * 1.5) {
			title = fmt.Sprintf("Volume Spike Alert: Ingestion volume increased by %.1f%%", vda.IngestionDeviation)
			message = fmt.Sprintf("Ingestion volume increased from %.0f events (%s) to %.0f events (%s) - %.1f%% increase",
				vda.YesterdayIngestion, yesterdayDate, vda.TodayIngestion, todayDate, vda.IngestionDeviation)
			functionality = alerts_async.LogSource
			errorCode = alerts_async.DGRW10001
		} else {
			title = fmt.Sprintf("Volume Spike Alert: Delivery volume increased by %.1f%%", vda.DeliveryDeviation)
			message = fmt.Sprintf("Delivery volume increased from %.0f events (%s) to %.0f events (%s) - %.1f%% increase",
				vda.YesterdayDelivery, yesterdayDate, vda.TodayDelivery, todayDate, vda.DeliveryDeviation)
			functionality = alerts_async.Dispenser
			errorCode = alerts_async.DGRW10001
		}
	}

	return alerts_async.NewAlert(
		functionality,
		alerts_async.WithEntity(vda),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.IngestionChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(errorCode, "Volume deviation detected"),
	)
}
