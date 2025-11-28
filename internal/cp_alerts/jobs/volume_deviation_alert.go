package jobs

import (
	"context"
	"fmt"
	"time"

	cpcommon "github.com/databahn-ai/databahn-jobs/internal/common"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/constants"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/entities"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_async"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
)

func SendAlertForVolumeDeviation(ctx context.Context) cpcommon.JobResult {
	db := config.GetDB()
	osClient := os.GetClient()

	tenants, err := tenant.GetTenants(ctx, db)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error getting all tenants", zap.Error(err))
		return cpcommon.NewJobResultFromError(err)
	}

	alertsManager, err := alert.NewAlertsManager(ctx)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Error getting alerts manager", zap.Error(err))
		return cpcommon.NewJobResultFromError(err)
	}

	defer func() {
		alertsManager.Close(ctx)
	}()

	baseDateRange := calculateDailyVolumeDeviationDateRange()
	logger.GetLoggerWithContext(ctx).Info("checking volume deviation for date range", zap.Time("dateRangeStart", baseDateRange.DayToCheckStart), zap.Time("dateRangeEnd", baseDateRange.DayToCheckEnd))

	for _, t := range tenants {
		ingestionAlertsCount := 0
		deliveryAlertsCount := 0
		tenantIdUuid := t.Id
		tenantId := t.Id.String()
		logger.GetLoggerWithContext(ctx).Info("checking volume deviation for tenant", zap.String("tenant_id", tenantId))

		// Fetch tenant-level volume deviation config
		tenantConfig, err := getVolumeDeviationConfigForTenant(db, t.Id)
		var dateRange DailyVolumeDeviationDateRange
		if err != nil {
			logger.GetLogger().Error("failed to fetch tenant-level volume deviation config, skipping tenant",
				zap.String("tenantId", tenantId), zap.Error(err))
			continue
		}

		if tenantConfig == nil || tenantConfig.Config == nil || !tenantConfig.Config.Enabled {
			// No config found or config is disabled - use defaults from environment variables
			logger.GetLogger().Info("no tenant-level volume deviation config found or disabled, using defaults",
				zap.String("tenantId", tenantId))
			dateRange = baseDateRange
		} else {
			// Use tenant-specific configuration
			alertConfig := tenantConfig.Config.VolumeDeviationAlertConfig
			if alertConfig == nil {
				logger.GetLogger().Warn("tenant config exists but VolumeDeviationAlertConfig is nil, using defaults",
					zap.String("tenantId", tenantId))
				dateRange = baseDateRange
			} else {
				// Override the thresholds with tenant config
				dateRange = baseDateRange
				dateRange.PercentageIncreaseThreshold = float64(alertConfig.PercentageThreshold)
				dateRange.PercentageDecreaseThreshold = float64(alertConfig.PercentageThreshold)

				// Convert minimum difference volume to bytes
				minDiffVolumeBytes := util.DataVolumeToBytes(alertConfig.MinimumDifferenceVolume, alertConfig.MinimumDifferenceVolumeUnit)
				dateRange.MinimumVolumeThreshold = float64(minDiffVolumeBytes)

				logger.GetLogger().Info("using tenant-level volume deviation config",
					zap.String("tenantId", tenantId),
					zap.Float64("percentageThreshold", float64(alertConfig.PercentageThreshold)),
					zap.Int64("minimumDifferenceVolume", alertConfig.MinimumDifferenceVolume),
					zap.String("minimumDifferenceVolumeUnit", alertConfig.MinimumDifferenceVolumeUnit),
					zap.Int64("minimumDifferenceVolumeBytes", minDiffVolumeBytes))
			}
		}

		ingestionStats, err := getIngestionStats(ctx, osClient, tenantId, &dateRange)
		if err != nil {
			return cpcommon.NewJobResultFromError(err)
		}

		sourceIdToIngestionStats := make(map[string]*DailyVolumeDeviationIngestionStats)
		for _, ingestionStat := range ingestionStats {
			sourceIdToIngestionStats[ingestionStat.SourceId] = ingestionStat
		}

		var ingestionAlerts []*alerts_async.Alert
		sourceDbPage := 0
		sourceDbPageSize := 50
		for {
			sources, err := source.ReadSourcesPaginated(db, tenantIdUuid, sourceDbPage, sourceDbPageSize)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("error while reading sources", zap.Error(err))
				return cpcommon.NewJobResultFromError(err)
			}
			if len(sources) == 0 {
				break
			}
			for _, source := range sources {
				ingestionStat := sourceIdToIngestionStats[source.ID.String()]
				if ingestionStat == nil {
					logger.GetLoggerWithContext(ctx).Error("ingestion stat not found for source", zap.String("sourceId", source.ID.String()), zap.String("tenantId", tenantId))
					continue
				}
				ingestionSource := DailyVolumeDeviationIngestionSource{
					DailyVolumeDeviationIngestionStats: *ingestionStat,
					Source:                             &source,
				}
				alert, err := ingestionSource.checkVolumeDeviation()
				if err != nil {
					return cpcommon.NewJobResultFromError(err)
				}
				if alert != nil {
					logger.GetLogger().Info("volume deviation alert generated for source", zap.String("sourceId", source.ID.String()), zap.String("tenantId", tenantId))
					ingestionAlerts = append(ingestionAlerts, alert)
				}

			}
			sourceDbPage++
		}
		if len(ingestionAlerts) > 0 {
			ingestionAlertsCount += len(ingestionAlerts)
			err = alertsManager.SendAlerts(ingestionAlerts)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("error while sending ingestion alerts", zap.Error(err))
				return cpcommon.NewJobResultFromError(err)
			}
		} else {
			logger.GetLoggerWithContext(ctx).Info("no ingestion alerts to send for tenant", zap.String("tenantId", tenantId))
		}
		logger.GetLoggerWithContext(ctx).Info("done processing sources", zap.String("tenantId", tenantId), zap.Int("ingestionAlerts", len(ingestionAlerts)))

		deliveryStats, err := getDeliveryStats(ctx, osClient, tenantId, &dateRange)
		if err != nil {
			return cpcommon.NewJobResultFromError(err)
		}

		deliveryAlerts := make([]*alerts_async.Alert, 0)
		destinationIdToDeliveryStats := make(map[string]*DailyVolumeDeviationDeliveryStats)
		for _, deliveryStat := range deliveryStats {
			destinationIdToDeliveryStats[deliveryStat.DestinationId] = deliveryStat
		}
		destinationDbPage := 0
		destinationDbPageSize := 50
		for {
			destinations, err := readDestinationsPaginated(db, tenantIdUuid, destinationDbPage, destinationDbPageSize)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("error while reading destinations", zap.Error(err))
				return cpcommon.NewJobResultFromError(err)
			}
			if len(destinations) == 0 {
				break
			}
			for _, destination := range destinations {
				// Skip alert if destination is the Databahn Sandbox and tenant has disabled sandbox alerts
				if destination.ID.String() == constants.SandboxDestinationID {
					shouldSkip, err := entities.ShouldSkipSandboxAlerts(db, tenantIdUuid)
					if err != nil {
						logger.GetLoggerWithContext(ctx).Error("error checking sandbox alerts config, skipping sandbox alerts as fail-safe",
							zap.Error(err),
							zap.String("tenantId", tenantId),
							zap.String("destinationId", destination.ID.String()))
						continue
					}
					if shouldSkip {
						logger.GetLoggerWithContext(ctx).Info("skipping volume deviation check for sandbox destination (tenant has disabled sandbox alerts)",
							zap.String("destinationId", destination.ID.String()),
							zap.String("destinationName", destination.Name),
							zap.String("tenantId", tenantId))
						continue
					}
				}

				deliveryStat := destinationIdToDeliveryStats[destination.ID.String()]
				if deliveryStat == nil {
					logger.GetLoggerWithContext(ctx).Error("delivery stat not found for destination", zap.String("destinationId", destination.ID.String()), zap.String("tenantId", tenantId))
					continue
				}
				deliveryDestination := DailyVolumeDeviationDeliveryDestination{
					DailyVolumeDeviationDeliveryStats: *deliveryStat,
					Destination:                       &destination,
				}
				alert, err := deliveryDestination.checkVolumeDeviation()
				if err != nil {
					return cpcommon.NewJobResultFromError(err)
				}
				if alert != nil {
					logger.GetLogger().Info("volume deviation alert generated for destination", zap.String("destinationId", destination.ID.String()), zap.String("tenantId", tenantId))
					deliveryAlerts = append(deliveryAlerts, alert)
				}
			}
			destinationDbPage++
		}
		if len(deliveryAlerts) > 0 {
			deliveryAlertsCount += len(deliveryAlerts)
			err = alertsManager.SendAlerts(deliveryAlerts)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("error while sending delivery alerts", zap.Error(err))
				return cpcommon.NewJobResultFromError(err)
			}
		}
		logger.GetLoggerWithContext(ctx).Info("done processing destinations", zap.String("tenantId", tenantId), zap.Int("deliveryAlerts", len(deliveryAlerts)))

		logger.GetLoggerWithContext(ctx).Info("done processing volume deviation alerts for tenant", zap.String("tenantId", tenantId),
			zap.Int("ingestionAlerts", ingestionAlertsCount), zap.Int("deliveryAlerts", deliveryAlertsCount))

	}

	return cpcommon.NewJobResultSuccess()
}

type DailyVolumeDeviationDateRange struct {
	DayToCheckStart              time.Time
	DayToCheckEnd                time.Time
	LastDayToCompareStart        time.Time
	LastDayToCompareEnd          time.Time
	DayToCheckLastWeekDayStart   time.Time
	DayToCheckLastWeekDayEnd     time.Time
	DayToCompareLastWeekDayStart time.Time
	DayToCompareLastWeekDayEnd   time.Time
	PercentageIncreaseThreshold  float64
	PercentageDecreaseThreshold  float64
	MinimumVolumeThreshold       float64
}

type DailyVolumeDeviationIngestionStats struct {
	DailyVolumeDeviationDateRange
	TenantId                          string
	SourceId                          string
	CheckDayVolumeIngestion           float64
	LastDayToCompareIngestion         float64
	LastWeekSameDayToCheckIngestion   float64
	LastWeekSameDayToCompareIngestion float64
}

type DailyVolumeDeviationIngestionSource struct {
	DailyVolumeDeviationIngestionStats
	Source *source.Source
	alerts_async.NoSecondaryEntityId
}

func (d *DailyVolumeDeviationIngestionSource) GetEntityId() string {
	return d.SourceId
}

func (d *DailyVolumeDeviationIngestionSource) GetEntityName() string {
	return d.Source.Name
}

func (d *DailyVolumeDeviationIngestionSource) GetDataPlaneId() string {
	return d.Source.DataPlaneId.String()
}

func (d *DailyVolumeDeviationIngestionSource) GetTenantId() string {
	return d.TenantId
}

func (d *DailyVolumeDeviationIngestionSource) checkVolumeDeviation() (*alerts_async.Alert, error) {
	var alert *alerts_async.Alert = nil
	var alertIsForIncrease bool

	if d.CheckDayVolumeIngestion < d.DailyVolumeDeviationDateRange.MinimumVolumeThreshold && d.LastDayToCompareIngestion < d.DailyVolumeDeviationDateRange.MinimumVolumeThreshold {
		logger.GetLogger().Info("ignoring source with volume below minimum threshold", zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId), zap.Float64("checkDayVolumeIngestion", d.CheckDayVolumeIngestion), zap.Float64("lastDayToCompareIngestion", d.LastDayToCompareIngestion))
		return nil, nil
	}

	if d.Source.CreatedAt.After(d.DailyVolumeDeviationDateRange.LastDayToCompareStart) {
		logger.GetLogger().Info("ignoring source created after last day to compare start", zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId), zap.Time("sourceCreatedAt", d.Source.CreatedAt), zap.Time("lastDayToCompareStart", d.DailyVolumeDeviationDateRange.LastDayToCompareStart))
		return nil, nil
	} else {
		if d.CheckDayVolumeIngestion > d.LastDayToCompareIngestion {
			percentageIncrease := 0.0
			if d.LastDayToCompareIngestion == 0 {
				percentageIncrease = 100
			} else {
				percentageIncrease = ((d.CheckDayVolumeIngestion - d.LastDayToCompareIngestion) / d.LastDayToCompareIngestion) * 100
			}
			if percentageIncrease > d.DailyVolumeDeviationDateRange.PercentageIncreaseThreshold {
				incrAlert, err := d.buildAlert(true, percentageIncrease)
				if err != nil {
					logger.GetLogger().Error("error while building alert for source for last day to compare", zap.Error(err), zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId))
					return nil, err
				}
				alertIsForIncrease = true
				alert = incrAlert
			}
		} else {
			percentageDecrease := 0.0
			if d.LastDayToCompareIngestion == 0 {
				percentageDecrease = 100
			} else {
				percentageDecrease = ((d.LastDayToCompareIngestion - d.CheckDayVolumeIngestion) / d.LastDayToCompareIngestion) * 100
			}
			if percentageDecrease > d.DailyVolumeDeviationDateRange.PercentageDecreaseThreshold {
				decAlert, err := d.buildAlert(false, percentageDecrease)
				if err != nil {
					logger.GetLogger().Error("error while building alert for source for last day to compare", zap.Error(err), zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId))
					return nil, err
				}
				alertIsForIncrease = false
				alert = decAlert
			}
		}
	}

	if alert == nil {
		logger.GetLogger().Info("no volume deviation detected for source for last day to compare", zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId),
			zap.Float64("checkDayVolumeIngestion", d.CheckDayVolumeIngestion), zap.Float64("lastDayToCompareIngestion", d.LastDayToCompareIngestion))
		return nil, nil
	}

	hadSimilarDeviationLastWeekSameDay := false

	if d.LastWeekSameDayToCheckIngestion < d.DailyVolumeDeviationDateRange.MinimumVolumeThreshold && d.LastWeekSameDayToCompareIngestion < d.DailyVolumeDeviationDateRange.MinimumVolumeThreshold {
		logger.GetLogger().Info("skipping last week comparison for source with volume below minimum threshold", zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId), zap.Float64("lastWeekSameDayToCheckIngestion", d.LastWeekSameDayToCheckIngestion), zap.Float64("lastWeekSameDayToCompareIngestion", d.LastWeekSameDayToCompareIngestion))
		return alert, nil
	} else {
		if d.Source.CreatedAt.After(d.DailyVolumeDeviationDateRange.DayToCompareLastWeekDayStart) {
			logger.GetLogger().Info("ignoring source created after last week same day to compare start", zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId), zap.Time("sourceCreatedAt", d.Source.CreatedAt), zap.Time("lastWeekSameDayToCompareStart", d.DailyVolumeDeviationDateRange.DayToCheckLastWeekDayStart))
		} else {
			if alertIsForIncrease && d.LastWeekSameDayToCheckIngestion > d.LastWeekSameDayToCompareIngestion {
				percentageIncrease := 0.0
				if d.LastWeekSameDayToCompareIngestion == 0 {
					percentageIncrease = 100
				} else {
					percentageIncrease = ((d.LastWeekSameDayToCheckIngestion - d.LastWeekSameDayToCompareIngestion) / d.LastWeekSameDayToCompareIngestion) * 100
				}
				if percentageIncrease > d.DailyVolumeDeviationDateRange.PercentageIncreaseThreshold {
					hadSimilarDeviationLastWeekSameDay = true
				}
			} else if !alertIsForIncrease {
				percentageDecrease := 0.0
				if d.LastWeekSameDayToCompareIngestion == 0 {
					percentageDecrease = 100
				} else {
					percentageDecrease = ((d.LastWeekSameDayToCompareIngestion - d.LastWeekSameDayToCheckIngestion) / d.LastWeekSameDayToCompareIngestion) * 100
				}
				if percentageDecrease > d.DailyVolumeDeviationDateRange.PercentageDecreaseThreshold {
					hadSimilarDeviationLastWeekSameDay = true
				}
			}
		}
	}

	if hadSimilarDeviationLastWeekSameDay {
		logger.GetLogger().Info("had similar deviation last week same day to compare, ignoring alert", zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId),
			zap.Float64("checkDayVolumeIngestion", d.CheckDayVolumeIngestion), zap.Float64("lastWeekSameDayToCheckIngestion", d.LastWeekSameDayToCheckIngestion))
		return nil, nil
	}

	logger.GetLogger().Info("ingestion volume deviation detected", zap.String("sourceId", d.SourceId), zap.String("tenantId", d.TenantId))
	return alert, nil
}

func (d *DailyVolumeDeviationIngestionSource) buildAlert(volumeIncr bool, volumeChangePercent float64) (*alerts_async.Alert, error) {
	title := ""
	message := ""
	dayToCheck := d.DailyVolumeDeviationDateRange.DayToCheckStart.Format("2006-01-02")
	dayToCompare := d.DailyVolumeDeviationDateRange.LastDayToCompareStart.Format("2006-01-02")
	volumeToCheck := d.CheckDayVolumeIngestion
	volumeToCompare := d.LastDayToCompareIngestion
	if volumeIncr {
		title = fmt.Sprintf("Volume Spike Alert: Ingestion volume increased by %.1f%%", volumeChangePercent)
		message = fmt.Sprintf("Ingestion volume increased by %.1f%% from %s on %s to %s on %s for source '%s'", volumeChangePercent,
			util.HumanReadableBytes(int64(volumeToCompare)), dayToCompare, util.HumanReadableBytes(int64(volumeToCheck)), dayToCheck, d.Source.Name)
	} else {
		title = fmt.Sprintf("Volume Drop Alert: Ingestion volume decreased by %.1f%%", volumeChangePercent)
		message = fmt.Sprintf("Ingestion volume decreased by %.1f%% from %s on %s to %s on %s for source '%s'", volumeChangePercent,
			util.HumanReadableBytes(int64(volumeToCompare)), dayToCompare, util.HumanReadableBytes(int64(volumeToCheck)), dayToCheck, d.Source.Name)
	}
	return alerts_async.NewAlert(
		alerts_async.LogSource,
		alerts_async.WithEntity(d),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.DataRatioAlertChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10005, "Unusual ingestion volume deviation detected."),
		alerts_async.WithAction("Please check source configuration or original source of the data for any reasons to send more or less than usual data."),
	)
}

type DailyVolumeDeviationDeliveryStats struct {
	DailyVolumeDeviationDateRange
	TenantId                         string
	DestinationId                    string
	CheckDayVolumeDelivery           float64
	LastDayToCompareDelivery         float64
	LastWeekSameDayToCheckDelivery   float64
	LastWeekSameDayToCompareDelivery float64
	DestinationCreatedAt             time.Time
}

type DailyVolumeDeviationDeliveryDestination struct {
	DailyVolumeDeviationDeliveryStats
	Destination *destination.Destination
	alerts_async.NoSecondaryEntityId
}

func (d *DailyVolumeDeviationDeliveryDestination) GetEntityId() string {
	return d.DestinationId
}

func (d *DailyVolumeDeviationDeliveryDestination) GetEntityName() string {
	return d.Destination.Name
}

func (d *DailyVolumeDeviationDeliveryDestination) GetDataPlaneId() string {
	return d.Destination.DataPlaneId.String()
}

func (d *DailyVolumeDeviationDeliveryDestination) GetTenantId() string {
	return d.TenantId
}

func (d *DailyVolumeDeviationDeliveryDestination) checkVolumeDeviation() (*alerts_async.Alert, error) {
	var alert *alerts_async.Alert = nil
	var alertIsForIncrease bool

	if d.CheckDayVolumeDelivery < d.DailyVolumeDeviationDateRange.MinimumVolumeThreshold && d.LastDayToCompareDelivery < d.DailyVolumeDeviationDateRange.MinimumVolumeThreshold {
		logger.GetLogger().Info("ignoring destination with volume below minimum threshold", zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId), zap.Float64("checkDayVolumeDelivery", d.CheckDayVolumeDelivery), zap.Float64("lastDayToCompareDelivery", d.LastDayToCompareDelivery))
		return nil, nil
	}

	if d.Destination.CreatedAt.After(d.DailyVolumeDeviationDateRange.LastDayToCompareStart) {
		logger.GetLogger().Info("ignoring destination created after last day to compare start", zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId), zap.Time("destinationCreatedAt", d.Destination.CreatedAt), zap.Time("lastDayToCompareStart", d.DailyVolumeDeviationDateRange.LastDayToCompareStart))
		return nil, nil
	} else {
		if d.CheckDayVolumeDelivery > d.LastDayToCompareDelivery {
			percentageIncrease := 0.0
			if d.LastDayToCompareDelivery == 0 {
				percentageIncrease = 100
			} else {
				percentageIncrease = ((d.CheckDayVolumeDelivery - d.LastDayToCompareDelivery) / d.LastDayToCompareDelivery) * 100
			}
			if percentageIncrease > d.DailyVolumeDeviationDateRange.PercentageIncreaseThreshold {
				incrAlert, err := d.buildAlert(true, percentageIncrease)
				if err != nil {
					logger.GetLogger().Error("error while building alert for destination for last day to compare", zap.Error(err), zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId))
					return nil, err
				}
				alertIsForIncrease = true
				alert = incrAlert
			}
		} else {
			percentageDecrease := 0.0
			if d.LastDayToCompareDelivery == 0 {
				percentageDecrease = 100
			} else {
				percentageDecrease = ((d.LastDayToCompareDelivery - d.CheckDayVolumeDelivery) / d.LastDayToCompareDelivery) * 100
			}
			if percentageDecrease > d.DailyVolumeDeviationDateRange.PercentageDecreaseThreshold {
				decAlert, err := d.buildAlert(false, percentageDecrease)
				if err != nil {
					logger.GetLogger().Error("error while building alert for destination for last day to compare", zap.Error(err), zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId))
					return nil, err
				}
				alertIsForIncrease = false
				alert = decAlert
			}
		}
	}

	if alert == nil {
		logger.GetLogger().Info("no volume deviation detected for destination for last day to compare", zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId),
			zap.Float64("checkDayVolumeDelivery", d.CheckDayVolumeDelivery), zap.Float64("lastDayToCompareDelivery", d.LastDayToCompareDelivery))
		return nil, nil
	}

	hadSimilarDeviationLastWeekSameDay := false

	if d.LastWeekSameDayToCheckDelivery < d.DailyVolumeDeviationDateRange.MinimumVolumeThreshold && d.LastWeekSameDayToCompareDelivery < d.DailyVolumeDeviationDateRange.MinimumVolumeThreshold {
		logger.GetLogger().Info("skipping last week comparison for destination with volume below minimum threshold", zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId), zap.Float64("lastWeekSameDayToCheckDelivery", d.LastWeekSameDayToCheckDelivery), zap.Float64("lastWeekSameDayToCompareDelivery", d.LastWeekSameDayToCompareDelivery))
		return alert, nil
	} else {
		if d.Destination.CreatedAt.After(d.DailyVolumeDeviationDateRange.DayToCompareLastWeekDayStart) {
			logger.GetLogger().Info("ignoring destination created after last week same day to compare start", zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId), zap.Time("destinationCreatedAt", d.Destination.CreatedAt), zap.Time("lastWeekSameDayToCompareStart", d.DailyVolumeDeviationDateRange.DayToCheckLastWeekDayStart))
		} else {
			if alertIsForIncrease && d.LastWeekSameDayToCheckDelivery > d.LastWeekSameDayToCompareDelivery {
				percentageIncrease := 0.0
				if d.LastWeekSameDayToCompareDelivery == 0 {
					percentageIncrease = 100
				} else {
					percentageIncrease = ((d.LastWeekSameDayToCheckDelivery - d.LastWeekSameDayToCompareDelivery) / d.LastWeekSameDayToCompareDelivery) * 100
				}
				if percentageIncrease > d.DailyVolumeDeviationDateRange.PercentageIncreaseThreshold {
					hadSimilarDeviationLastWeekSameDay = true
				}
			} else if !alertIsForIncrease {
				percentageDecrease := 0.0
				if d.LastWeekSameDayToCompareDelivery == 0 {
					percentageDecrease = 100
				} else {
					percentageDecrease = ((d.LastWeekSameDayToCompareDelivery - d.LastWeekSameDayToCheckDelivery) / d.LastWeekSameDayToCompareDelivery) * 100
				}
				if percentageDecrease > d.DailyVolumeDeviationDateRange.PercentageDecreaseThreshold {
					hadSimilarDeviationLastWeekSameDay = true
				}
			}
		}
	}

	if hadSimilarDeviationLastWeekSameDay {
		logger.GetLogger().Info("had similar deviation last week same day to compare, ignoring alert", zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId),
			zap.Float64("checkDayVolumeDelivery", d.CheckDayVolumeDelivery), zap.Float64("lastWeekSameDayToCheckDelivery", d.LastWeekSameDayToCheckDelivery))
		return nil, nil
	}

	logger.GetLogger().Info("delivery volume deviation detected", zap.String("destinationId", d.DestinationId), zap.String("tenantId", d.TenantId))
	return alert, nil
}

func (d *DailyVolumeDeviationDeliveryDestination) buildAlert(volumeIncr bool, volumeChangePercent float64) (*alerts_async.Alert, error) {
	title := ""
	message := ""
	dayToCheck := d.DailyVolumeDeviationDateRange.DayToCheckStart.Format("2006-01-02")
	dayToCompare := d.DailyVolumeDeviationDateRange.LastDayToCompareStart.Format("2006-01-02")
	volumeToCheck := d.CheckDayVolumeDelivery
	volumeToCompare := d.LastDayToCompareDelivery
	if volumeIncr {
		title = fmt.Sprintf("Volume Spike Alert: Delivery volume increased by %.1f%%", volumeChangePercent)
		message = fmt.Sprintf("Delivery volume increased by %.1f%% from %s on %s to %s on %s for destination '%s'", volumeChangePercent,
			util.HumanReadableBytes(int64(volumeToCompare)), dayToCompare, util.HumanReadableBytes(int64(volumeToCheck)), dayToCheck, d.Destination.Name)
	} else {
		title = fmt.Sprintf("Volume Drop Alert: Delivery volume decreased by %.1f%%", volumeChangePercent)
		message = fmt.Sprintf("Delivery volume decreased by %.1f%% from %s on %s to %s on %s for destination '%s'", volumeChangePercent,
			util.HumanReadableBytes(int64(volumeToCompare)), dayToCompare, util.HumanReadableBytes(int64(volumeToCheck)), dayToCheck, d.Destination.Name)
	}
	return alerts_async.NewAlert(
		alerts_async.Dispenser,
		alerts_async.WithEntity(d),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.DataRatioAlertChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10005, "Unusual delivery volume deviation detected."),
		alerts_async.WithAction("Please check any volume control rule changes, any source ingestion volume changes, transformation changes or destination configuration changes which can cause such unusual drop or spike in destination volume."),
	)
}

func calculateDailyVolumeDeviationDateRange() DailyVolumeDeviationDateRange {
	now := time.Now().UTC()
	runForToday := utils.GetEnvOrDefault("VOLUME_DEVIATION_ALERT_USE_CURRENT_DAY", "false") == "true"
	dayToCheckStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	dayToCheckEnd := dayToCheckStart.Add(24 * time.Hour).Add(-1 * time.Second)
	if runForToday {
		dayToCheckStart = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		dayToCheckEnd = dayToCheckStart.Add(24 * time.Hour).Add(-1 * time.Second)
	}
	dayToCompareStart := dayToCheckStart.AddDate(0, 0, -1)
	dayToCompareEnd := dayToCompareStart.Add(24 * time.Hour).Add(-1 * time.Second)

	dateToCheckLastWeekSameDayStart := dayToCheckStart.AddDate(0, 0, -7)
	dateToCheckLastWeekSameDayEnd := dateToCheckLastWeekSameDayStart.Add(24 * time.Hour).Add(-1 * time.Second)

	dateToCompareLastWeekSameDayStart := dayToCompareStart.AddDate(0, 0, -7)
	dateToCompareLastWeekSameDayEnd := dateToCompareLastWeekSameDayStart.Add(24 * time.Hour).Add(-1 * time.Second)

	percentageIncreaseThreshold := float64(utils.GetEnvInt("VOLUME_DEVIATION_PERCENTAGE_INCREASE_THRESHOLD", 50))
	percentageDecreaseThreshold := float64(utils.GetEnvInt("VOLUME_DEVIATION_PERCENTAGE_DECREASE_THRESHOLD", 50))
	minimumVolumeThreshold := float64(utils.GetEnvInt("VOLUME_DEVIATION_MINIMUM_VOLUME_THRESHOLD", 1_000_000))

	return DailyVolumeDeviationDateRange{
		DayToCheckStart:              dayToCheckStart,
		DayToCheckEnd:                dayToCheckEnd,
		LastDayToCompareStart:        dayToCompareStart,
		LastDayToCompareEnd:          dayToCompareEnd,
		DayToCheckLastWeekDayStart:   dateToCheckLastWeekSameDayStart,
		DayToCheckLastWeekDayEnd:     dateToCheckLastWeekSameDayEnd,
		DayToCompareLastWeekDayStart: dateToCompareLastWeekSameDayStart,
		DayToCompareLastWeekDayEnd:   dateToCompareLastWeekSameDayEnd,
		PercentageIncreaseThreshold:  percentageIncreaseThreshold,
		PercentageDecreaseThreshold:  percentageDecreaseThreshold,
		MinimumVolumeThreshold:       minimumVolumeThreshold,
	}
}

func getIngestionStats(ctx context.Context, osClient *opensearch.Client, tenantId string, dateRange *DailyVolumeDeviationDateRange) ([]*DailyVolumeDeviationIngestionStats, error) {
	logSourceIdToInVolumeForCheckDay, err := findIngestedVolumesForSources(ctx, tenantId, osClient, dateRange.DayToCheckStart.UnixMilli(), dateRange.DayToCheckEnd.UnixMilli())
	if err != nil {
		logger.GetLogger().Error("error while finding ingested volumes per source", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}
	logSourceIdToInVolumeForLastDayToCompare, err := findIngestedVolumesForSources(ctx, tenantId, osClient, dateRange.LastDayToCompareStart.UnixMilli(), dateRange.LastDayToCompareEnd.UnixMilli())
	if err != nil {
		logger.GetLogger().Error("error while finding ingested volumes per source", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}
	logSourceIdToInVolumeForLastWeekSameDayToCheck, err := findIngestedVolumesForSources(ctx, tenantId, osClient, dateRange.DayToCheckLastWeekDayStart.UnixMilli(), dateRange.DayToCheckLastWeekDayEnd.UnixMilli())
	if err != nil {
		logger.GetLogger().Error("error while finding ingested volumes per source", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}
	logSourceIdToInVolumeForLastWeekSameDayToCompare, err := findIngestedVolumesForSources(ctx, tenantId, osClient, dateRange.DayToCompareLastWeekDayStart.UnixMilli(), dateRange.DayToCompareLastWeekDayEnd.UnixMilli())
	if err != nil {
		logger.GetLogger().Error("error while finding ingested volumes per source", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}

	uniqueSourceIds := make(map[string]bool)
	for sourceId := range logSourceIdToInVolumeForCheckDay {
		uniqueSourceIds[sourceId] = true
	}
	for sourceId := range logSourceIdToInVolumeForLastDayToCompare {
		uniqueSourceIds[sourceId] = true
	}
	for sourceId := range logSourceIdToInVolumeForLastWeekSameDayToCheck {
		uniqueSourceIds[sourceId] = true
	}
	for sourceId := range logSourceIdToInVolumeForLastWeekSameDayToCompare {
		uniqueSourceIds[sourceId] = true
	}

	var ingestionStatsList []*DailyVolumeDeviationIngestionStats
	for sourceId := range uniqueSourceIds {
		ingestionStats := DailyVolumeDeviationIngestionStats{
			DailyVolumeDeviationDateRange:     *dateRange,
			TenantId:                          tenantId,
			SourceId:                          sourceId,
			CheckDayVolumeIngestion:           float64(logSourceIdToInVolumeForCheckDay[sourceId]),
			LastDayToCompareIngestion:         float64(logSourceIdToInVolumeForLastDayToCompare[sourceId]),
			LastWeekSameDayToCheckIngestion:   float64(logSourceIdToInVolumeForLastWeekSameDayToCheck[sourceId]),
			LastWeekSameDayToCompareIngestion: float64(logSourceIdToInVolumeForLastWeekSameDayToCompare[sourceId]),
		}
		ingestionStatsList = append(ingestionStatsList, &ingestionStats)
	}

	return ingestionStatsList, nil
}

func getDeliveryStats(ctx context.Context, osClient *opensearch.Client, tenantId string, dateRange *DailyVolumeDeviationDateRange) ([]*DailyVolumeDeviationDeliveryStats, error) {
	deliveredVolumesForCheckDay, err := findDeliveredVolumesForDestinations(ctx, tenantId, osClient, dateRange.DayToCheckStart.UnixMilli(), dateRange.DayToCheckEnd.UnixMilli())
	if err != nil {
		logger.GetLogger().Error("error while finding delivered volumes per destination", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}
	destinationIdToOutVolumeForCheckDay := make(map[string]int64)
	for _, dv := range deliveredVolumesForCheckDay {
		destinationIdToOutVolumeForCheckDay[dv.DestinationId] += dv.OutData
	}

	deliveredVolumesForLastDayToCompare, err := findDeliveredVolumesForDestinations(ctx, tenantId, osClient, dateRange.LastDayToCompareStart.UnixMilli(), dateRange.LastDayToCompareEnd.UnixMilli())
	if err != nil {
		logger.GetLogger().Error("error while finding delivered volumes per destination", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}
	destinationIdToOutVolumeForLastDayToCompare := make(map[string]int64)
	for _, dv := range deliveredVolumesForLastDayToCompare {
		destinationIdToOutVolumeForLastDayToCompare[dv.DestinationId] += dv.OutData
	}

	deliveredVolumesForLastWeekSameDayToCheck, err := findDeliveredVolumesForDestinations(ctx, tenantId, osClient, dateRange.DayToCheckLastWeekDayStart.UnixMilli(), dateRange.DayToCheckLastWeekDayEnd.UnixMilli())
	if err != nil {
		logger.GetLogger().Error("error while finding delivered volumes per destination", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}
	destinationIdToOutVolumeForLastWeekSameDayToCheck := make(map[string]int64)
	for _, dv := range deliveredVolumesForLastWeekSameDayToCheck {
		destinationIdToOutVolumeForLastWeekSameDayToCheck[dv.DestinationId] += dv.OutData
	}

	deliveredVolumesForLastWeekSameDayToCompare, err := findDeliveredVolumesForDestinations(ctx, tenantId, osClient, dateRange.DayToCompareLastWeekDayStart.UnixMilli(), dateRange.DayToCompareLastWeekDayEnd.UnixMilli())
	if err != nil {
		logger.GetLogger().Error("error while finding delivered volumes per destination", zap.Error(err), zap.String("tenantId", tenantId))
		return nil, err
	}
	destinationIdToOutVolumeForLastWeekSameDayToCompare := make(map[string]int64)
	for _, dv := range deliveredVolumesForLastWeekSameDayToCompare {
		destinationIdToOutVolumeForLastWeekSameDayToCompare[dv.DestinationId] += dv.OutData
	}

	uniqueDestinationIds := make(map[string]bool)
	for destinationId := range destinationIdToOutVolumeForCheckDay {
		uniqueDestinationIds[destinationId] = true
	}
	for destinationId := range destinationIdToOutVolumeForLastDayToCompare {
		uniqueDestinationIds[destinationId] = true
	}
	for destinationId := range destinationIdToOutVolumeForLastWeekSameDayToCheck {
		uniqueDestinationIds[destinationId] = true
	}
	for destinationId := range destinationIdToOutVolumeForLastWeekSameDayToCompare {
		uniqueDestinationIds[destinationId] = true
	}

	var deliveryStatsList []*DailyVolumeDeviationDeliveryStats
	for destinationId := range uniqueDestinationIds {
		deliveryStats := DailyVolumeDeviationDeliveryStats{
			DailyVolumeDeviationDateRange:    *dateRange,
			TenantId:                         tenantId,
			DestinationId:                    destinationId,
			CheckDayVolumeDelivery:           float64(destinationIdToOutVolumeForCheckDay[destinationId]),
			LastDayToCompareDelivery:         float64(destinationIdToOutVolumeForLastDayToCompare[destinationId]),
			LastWeekSameDayToCheckDelivery:   float64(destinationIdToOutVolumeForLastWeekSameDayToCheck[destinationId]),
			LastWeekSameDayToCompareDelivery: float64(destinationIdToOutVolumeForLastWeekSameDayToCompare[destinationId]),
		}
		deliveryStatsList = append(deliveryStatsList, &deliveryStats)
	}

	return deliveryStatsList, nil
}
