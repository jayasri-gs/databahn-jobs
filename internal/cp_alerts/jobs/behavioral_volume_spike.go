package jobs

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	cpcommon "github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/cp_alerts/alert"
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

// BehavioralSpikeConfig holds configuration for behavioral spike detection
type BehavioralSpikeConfig struct {
	NumberOfDays           int
	MinimumVolumeThreshold int64
}

// getBehavioralSpikeConfig reads behavioral spike configuration from environment variables
func getBehavioralSpikeConfig() BehavioralSpikeConfig {
	numberOfDays := utils.GetEnvInt("BEHAVIORAL_SPIKE_NUMBER_OF_DAYS", 14)
	minimumVolumeThreshold := int64(utils.GetEnvInt("BEHAVIORAL_SPIKE_MINIMUM_VOLUME_THRESHOLD", 10_000_000))

	return BehavioralSpikeConfig{
		NumberOfDays:           numberOfDays,
		MinimumVolumeThreshold: minimumVolumeThreshold,
	}
}

func CheckBehavioralVolumeSpikes(ctx context.Context) cpcommon.JobResult {
	db := config.GetDB()
	osClient := os.GetClient()

	// Load configuration from environment variables
	spikeConfig := getBehavioralSpikeConfig()
	logger.GetLoggerWithContext(ctx).Info("behavioral spike configuration loaded",
		zap.Int("numberOfDays", spikeConfig.NumberOfDays),
		zap.Int64("minimumVolumeThreshold", spikeConfig.MinimumVolumeThreshold))

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

	logger.GetLoggerWithContext(ctx).Info("checking behavioral spikes for all tenants")

	for _, t := range tenants {
		sourceAlertsCount := 0
		destinationAlertsCount := 0
		tenantIdUuid := t.Id
		tenantId := t.Id.String()
		logger.GetLoggerWithContext(ctx).Info("checking behavioral spikes for tenant", zap.String("tenant_id", tenantId))

		// Process sources
		var sourceAlerts []*alerts_async.Alert
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

			for _, src := range sources {
				logger.GetLogger().Info("processing source for behavioral spikes",
					zap.String("sourceId", src.ID.String()),
					zap.String("sourceName", src.Name),
					zap.String("tenantId", tenantId))

				alert, err := checkSourceBehavioralSpike(ctx, tenantId, &src, &spikeConfig, osClient)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error checking source behavioral spike",
						zap.String("sourceId", src.ID.String()),
						zap.String("tenantId", tenantId),
						zap.Error(err))
					return cpcommon.NewJobResultFromError(err)
				}
				if alert != nil {
					logger.GetLoggerWithContext(ctx).Info("behavioral spike alert generated for source",
						zap.String("sourceId", src.ID.String()),
						zap.String("sourceName", src.Name),
						zap.String("tenantId", tenantId))
					sourceAlerts = append(sourceAlerts, alert)
					sourceAlertsCount++
				}
			}

			sourceDbPage++
		}

		// Send source alerts
		if len(sourceAlerts) > 0 {
			err = alertsManager.SendAlerts(sourceAlerts)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("error while sending source behavioral spike alerts", zap.Error(err))
				return cpcommon.NewJobResultFromError(err)
			}
		} else {
			logger.GetLoggerWithContext(ctx).Info("no source behavioral spike alerts to send for tenant", zap.String("tenantId", tenantId))
		}

		logger.GetLoggerWithContext(ctx).Info("done processing sources for behavioral spikes",
			zap.String("tenantId", tenantId),
			zap.Int("sourceAlertsCount", sourceAlertsCount))

		// Process destinations (dispensers)
		var destinationAlerts []*alerts_async.Alert
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

			for _, dest := range destinations {
				logger.GetLogger().Info("processing destination for behavioral spikes",
					zap.String("destinationId", dest.ID.String()),
					zap.String("destinationName", dest.Name),
					zap.String("tenantId", tenantId))

				alert, err := checkDestinationBehavioralSpike(ctx, tenantId, &dest, &spikeConfig, osClient)
				if err != nil {
					logger.GetLoggerWithContext(ctx).Error("error checking destination behavioral spike",
						zap.String("destinationId", dest.ID.String()),
						zap.String("tenantId", tenantId),
						zap.Error(err))
					return cpcommon.NewJobResultFromError(err)
				}
				if alert != nil {
					logger.GetLoggerWithContext(ctx).Info("behavioral spike alert generated for destination",
						zap.String("destinationId", dest.ID.String()),
						zap.String("destinationName", dest.Name),
						zap.String("tenantId", tenantId))
					destinationAlerts = append(destinationAlerts, alert)
					destinationAlertsCount++
				}
			}

			destinationDbPage++
		}

		// Send destination alerts
		if len(destinationAlerts) > 0 {
			err = alertsManager.SendAlerts(destinationAlerts)
			if err != nil {
				logger.GetLoggerWithContext(ctx).Error("error while sending destination behavioral spike alerts", zap.Error(err))
				return cpcommon.NewJobResultFromError(err)
			}
		} else {
			logger.GetLoggerWithContext(ctx).Info("no destination behavioral spike alerts to send for tenant", zap.String("tenantId", tenantId))
		}

		logger.GetLoggerWithContext(ctx).Info("done processing destinations for behavioral spikes",
			zap.String("tenantId", tenantId),
			zap.Int("destinationAlertsCount", destinationAlertsCount))

		logger.GetLoggerWithContext(ctx).Info("done processing behavioral spikes for tenant",
			zap.String("tenantId", tenantId),
			zap.Int("sourceAlerts", sourceAlertsCount),
			zap.Int("destinationAlerts", destinationAlertsCount))
	}

	logger.GetLoggerWithContext(ctx).Info("completed behavioral spike checks for all tenants")
	return cpcommon.NewJobResultSuccess()
}

// Placeholder functions for future implementation

// checkSourceBehavioralSpike analyzes historical data for a source and detects behavioral spikes
func checkSourceBehavioralSpike(ctx context.Context, tenantId string, src *source.Source, config *BehavioralSpikeConfig, osClient *opensearch.Client) (*alerts_async.Alert, error) {
	// Calculate time range: NumberOfDays + 1 (to have N days of history + 1 day to check)
	now := time.Now().UTC()
	daysToFetch := config.NumberOfDays + 1
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysToFetch)

	// Check if source was created after the time range start
	if src.CreatedAt.After(startTime) {
		logger.GetLogger().Info("ignoring source created after time range start",
			zap.String("sourceId", src.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Time("sourceCreatedAt", src.CreatedAt),
			zap.Time("timeRangeStart", startTime))
		return nil, nil
	}

	// Calculate time range: from (NumberOfDays + 1) days ago to yesterday end
	yesterday := now.Add(-24 * time.Hour)
	yesterdayEnd := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999000000, time.UTC).UnixMilli()
	fromTime := startTime.UnixMilli()

	// Query OpenSearch for historical volume data using GetHistogram with time range filter
	statsAlias := os.StatisticsIndexAlias(tenantId)
	query := fmt.Sprintf(`name:"total_data_received" AND tags.component_name:"storage" AND tags.db_event_source_id:"%s" AND tags.db_ts_win:[%d TO %d]`,
		src.ID.String(), fromTime, yesterdayEnd)

	agg := os.AggregationFunction{
		Name:     "total_bytes",
		Function: "sum",
		Field:    "counter.value",
	}

	histogram, err := os.GetHistogram(ctx, osClient, statsAlias, query, "tags.db_ts_win", agg, "1d")
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error fetching histogram for source",
			zap.String("sourceId", src.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Error(err))
		return nil, err
	}

	if len(histogram.Buckets) < 5 {
		logger.GetLogger().Info("insufficient data for behavioral spike detection",
			zap.String("sourceId", src.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Int("buckets", len(histogram.Buckets)))
		return nil, nil
	}

	// Sort buckets by time
	sort.Slice(histogram.Buckets, func(i, j int) bool {
		return histogram.Buckets[i].Time < histogram.Buckets[j].Time
	})

	// Check if last bucket time is yesterday (bucket timestamp is start of day)
	lastBucket := histogram.Buckets[len(histogram.Buckets)-1]
	yesterdayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()

	if lastBucket.Time != yesterdayStart {
		logger.GetLogger().Info("last bucket is not yesterday, skipping",
			zap.String("sourceId", src.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Int64("lastBucketTime", lastBucket.Time),
			zap.Int64("yesterdayStart", yesterdayStart),
			zap.String("lastBucketDate", time.UnixMilli(lastBucket.Time).Format("2006-01-02")),
			zap.String("yesterdayDate", time.UnixMilli(yesterdayStart).Format("2006-01-02")))
		return nil, nil
	}

	// Last day value to check
	lastDayValue := float64(lastBucket.Value)

	// Filter and collect historical values (excluding last day and values below threshold)
	var historicalValues []float64
	for i := 0; i < len(histogram.Buckets)-1; i++ {
		value := float64(histogram.Buckets[i].Value)
		if value >= float64(config.MinimumVolumeThreshold) {
			historicalValues = append(historicalValues, value)
		}
	}

	// Check if last day value is below threshold
	if lastDayValue < float64(config.MinimumVolumeThreshold) {
		logger.GetLogger().Info("last day volume below minimum threshold",
			zap.String("sourceId", src.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Float64("lastDayValue", lastDayValue),
			zap.Int64("minimumThreshold", config.MinimumVolumeThreshold))
		return nil, nil
	}

	if len(historicalValues) < 3 {
		logger.GetLogger().Info("insufficient historical data after filtering",
			zap.String("sourceId", src.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Int("historicalCount", len(historicalValues)))
		return nil, nil
	}

	// Calculate statistical measures
	mean := util.CalculateMean(historicalValues)
	stdDev := util.CalculateStandardDeviation(historicalValues, mean)

	logger.GetLogger().Info("behavioral spike statistics",
		zap.String("sourceId", src.ID.String()),
		zap.String("tenantId", tenantId),
		zap.Float64("mean", mean),
		zap.Float64("stdDev", stdDev),
		zap.Float64("lastDayValue", lastDayValue),
		zap.Int("historicalSampleSize", len(historicalValues)))

	// Check for zero or NaN standard deviation
	if stdDev == 0 || math.IsNaN(stdDev) {
		logger.GetLogger().Info("zero or NaN standard deviation, skipping",
			zap.String("sourceId", src.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Float64("stdDev", stdDev))
		return nil, nil
	}

	// Calculate z-score
	zScore := util.CalculateZScore(lastDayValue, mean, stdDev)
	threshold := dynamicThreshold(len(historicalValues))

	logger.GetLoggerWithContext(ctx).Info("z-score calculation for behavioral spike",
		zap.String("sourceId", src.ID.String()),
		zap.String("tenantId", tenantId),
		zap.Float64("zScore", zScore),
		zap.Float64("threshold", threshold))

	// Check if spike detected (positive z-score above threshold)
	if zScore > threshold {
		// Calculate expected range
		expectedMin := mean - (threshold * stdDev)
		expectedMax := mean + (threshold * stdDev)
		if expectedMin < 0 {
			expectedMin = 0
		}

		percentageIncrease := ((lastDayValue - mean) / mean) * 100

		logger.GetLoggerWithContext(ctx).Info("behavioral spike detected for source",
			zap.String("sourceId", src.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Float64("zScore", zScore),
			zap.Float64("percentageIncrease", percentageIncrease))

		return buildBehavioralSpikeAlertForSource(src, tenantId, lastDayValue, mean, stdDev, zScore, threshold, expectedMin, expectedMax, percentageIncrease)
	}

	return nil, nil
}

// dynamicThreshold returns the z-score threshold based on sample size
// Smaller samples need higher thresholds to avoid false positives
func dynamicThreshold(sampleSize int) float64 {
	if sampleSize < 10 {
		return 5.0
	} else if sampleSize < 30 {
		return 3.0
	} else {
		return 1.5
	}
}

// buildBehavioralSpikeAlertForSource creates an alert for behavioral spike detection in sources
func buildBehavioralSpikeAlertForSource(src *source.Source, tenantId string, actualValue, mean, stdDev, zScore, threshold, expectedMin, expectedMax, percentageIncrease float64) (*alerts_async.Alert, error) {
	title := fmt.Sprintf("Unusual Ingestion Spike Alert: Unusual ingestion volume spike detected (%.1f%% above normal)", percentageIncrease)
	message := fmt.Sprintf("Source '%s' ingested %s yesterday, which is significantly above the expected range of %s-%s (based on historical average of %s). This represents a %.1f%% increase from normal behavior.",
		src.Name,
		util.HumanReadableBytes(int64(actualValue)),
		util.HumanReadableBytes(int64(expectedMin)),
		util.HumanReadableBytes(int64(expectedMax)),
		util.HumanReadableBytes(int64(mean)),
		percentageIncrease)

	return alerts_async.NewAlert(
		alerts_async.LogSource,
		alerts_async.WithEntity(&behavioralSpikeSourceEntity{
			SourceId:    src.ID.String(),
			SourceName:  src.Name,
			TenantId:    tenantId,
			DataPlaneId: src.DataPlaneId.String(),
		}),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.VolumeAnomalyAlertChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10006, "Unusual spike detected in ingestion volume."),
		alerts_async.WithAction("Please investigate if this spike is expected. Check for new data sources added, changes in upstream systems, bulk data imports or system configuration changes."),
	)
}

// behavioralSpikeSourceEntity implements the entity interface for alerts
type behavioralSpikeSourceEntity struct {
	SourceId    string
	SourceName  string
	TenantId    string
	DataPlaneId string
	alerts_async.NoSecondaryEntityId
}

func (e *behavioralSpikeSourceEntity) GetEntityId() string {
	return e.SourceId
}

func (e *behavioralSpikeSourceEntity) GetEntityName() string {
	return e.SourceName
}

func (e *behavioralSpikeSourceEntity) GetTenantId() string {
	return e.TenantId
}

func (e *behavioralSpikeSourceEntity) GetDataPlaneId() string {
	return e.DataPlaneId
}

// checkDestinationBehavioralSpike analyzes historical data for a destination and detects behavioral spikes
func checkDestinationBehavioralSpike(ctx context.Context, tenantId string, dest *destination.Destination, config *BehavioralSpikeConfig, osClient *opensearch.Client) (*alerts_async.Alert, error) {
	// Calculate time range: NumberOfDays + 1 (to have N days of history + 1 day to check)
	now := time.Now().UTC()
	daysToFetch := config.NumberOfDays + 1
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysToFetch)

	// Check if destination was created after the time range start
	if dest.CreatedAt.After(startTime) {
		logger.GetLogger().Info("ignoring destination created after time range start",
			zap.String("destinationId", dest.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Time("destinationCreatedAt", dest.CreatedAt),
			zap.Time("timeRangeStart", startTime))
		return nil, nil
	}

	// Calculate time range: from (NumberOfDays + 1) days ago to yesterday end
	yesterday := now.Add(-24 * time.Hour)
	yesterdayEnd := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999000000, time.UTC).UnixMilli()
	fromTime := startTime.UnixMilli()

	// Query OpenSearch for historical volume data using GetHistogram with time range filter
	statsAlias := os.StatisticsIndexAlias(tenantId)
	query := fmt.Sprintf(`name:"total_bytes_delivered" AND tags.component_name:"dispenser" AND tags.destination_id:"%s" AND tags.db_ts_win:[%d TO %d]`,
		dest.ID.String(), fromTime, yesterdayEnd)

	agg := os.AggregationFunction{
		Name:     "total_bytes",
		Function: "sum",
		Field:    "counter.value",
	}

	histogram, err := os.GetHistogram(ctx, osClient, statsAlias, query, "tags.db_ts_win", agg, "1d")
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error fetching histogram for destination",
			zap.String("destinationId", dest.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Error(err))
		return nil, err
	}

	if len(histogram.Buckets) < 5 {
		logger.GetLogger().Info("insufficient data for behavioral spike detection",
			zap.String("destinationId", dest.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Int("buckets", len(histogram.Buckets)))
		return nil, nil
	}

	// Sort buckets by time
	sort.Slice(histogram.Buckets, func(i, j int) bool {
		return histogram.Buckets[i].Time < histogram.Buckets[j].Time
	})

	// Check if last bucket time is yesterday (bucket timestamp is start of day)
	lastBucket := histogram.Buckets[len(histogram.Buckets)-1]
	yesterdayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()

	if lastBucket.Time != yesterdayStart {
		logger.GetLogger().Info("last bucket is not yesterday, skipping",
			zap.String("destinationId", dest.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Int64("lastBucketTime", lastBucket.Time),
			zap.Int64("yesterdayStart", yesterdayStart),
			zap.String("lastBucketDate", time.UnixMilli(lastBucket.Time).Format("2006-01-02")),
			zap.String("yesterdayDate", time.UnixMilli(yesterdayStart).Format("2006-01-02")))
		return nil, nil
	}

	// Last day value to check
	lastDayValue := float64(lastBucket.Value)

	// Filter and collect historical values (excluding last day and values below threshold)
	var historicalValues []float64
	for i := 0; i < len(histogram.Buckets)-1; i++ {
		value := float64(histogram.Buckets[i].Value)
		if value >= float64(config.MinimumVolumeThreshold) {
			historicalValues = append(historicalValues, value)
		}
	}

	// Check if last day value is below threshold
	if lastDayValue < float64(config.MinimumVolumeThreshold) {
		logger.GetLogger().Info("last day volume below minimum threshold",
			zap.String("destinationId", dest.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Float64("lastDayValue", lastDayValue),
			zap.Int64("minimumThreshold", config.MinimumVolumeThreshold))
		return nil, nil
	}

	if len(historicalValues) < 3 {
		logger.GetLogger().Info("insufficient historical data after filtering",
			zap.String("destinationId", dest.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Int("historicalCount", len(historicalValues)))
		return nil, nil
	}

	// Calculate statistical measures
	mean := util.CalculateMean(historicalValues)
	stdDev := util.CalculateStandardDeviation(historicalValues, mean)

	logger.GetLogger().Info("behavioral spike statistics",
		zap.String("destinationId", dest.ID.String()),
		zap.String("tenantId", tenantId),
		zap.Float64("mean", mean),
		zap.Float64("stdDev", stdDev),
		zap.Float64("lastDayValue", lastDayValue),
		zap.Int("historicalSampleSize", len(historicalValues)))

	// Check for zero or NaN standard deviation
	if stdDev == 0 || math.IsNaN(stdDev) {
		logger.GetLogger().Info("zero or NaN standard deviation, skipping",
			zap.String("destinationId", dest.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Float64("stdDev", stdDev))
		return nil, nil
	}

	// Calculate z-score
	zScore := util.CalculateZScore(lastDayValue, mean, stdDev)
	threshold := dynamicThreshold(len(historicalValues))

	logger.GetLoggerWithContext(ctx).Info("z-score calculation for behavioral spike",
		zap.String("destinationId", dest.ID.String()),
		zap.String("tenantId", tenantId),
		zap.Float64("zScore", zScore),
		zap.Float64("threshold", threshold))

	// Check if spike detected (positive z-score above threshold)
	if zScore > threshold {
		// Calculate expected range
		expectedMin := mean - (threshold * stdDev)
		expectedMax := mean + (threshold * stdDev)
		if expectedMin < 0 {
			expectedMin = 0
		}

		percentageIncrease := ((lastDayValue - mean) / mean) * 100

		logger.GetLoggerWithContext(ctx).Info("behavioral spike detected for destination",
			zap.String("destinationId", dest.ID.String()),
			zap.String("tenantId", tenantId),
			zap.Float64("zScore", zScore),
			zap.Float64("percentageIncrease", percentageIncrease))

		return buildBehavioralSpikeAlertForDestination(dest, tenantId, lastDayValue, mean, stdDev, zScore, threshold, expectedMin, expectedMax, percentageIncrease)
	}

	return nil, nil
}

// buildBehavioralSpikeAlertForDestination creates an alert for behavioral spike detection in destinations
func buildBehavioralSpikeAlertForDestination(dest *destination.Destination, tenantId string, actualValue, mean, stdDev, zScore, threshold, expectedMin, expectedMax, percentageIncrease float64) (*alerts_async.Alert, error) {
	title := fmt.Sprintf("Unusual Delivery Spike Alert: Unusual delivery volume spike detected (%.1f%% above normal)", percentageIncrease)
	message := fmt.Sprintf("Destination '%s' delivered %s yesterday, which is significantly above the expected range of %s-%s (based on historical average of %s). This represents a %.1f%% increase from normal behavior.",
		dest.Name,
		util.HumanReadableBytes(int64(actualValue)),
		util.HumanReadableBytes(int64(expectedMin)),
		util.HumanReadableBytes(int64(expectedMax)),
		util.HumanReadableBytes(int64(mean)),
		percentageIncrease)

	return alerts_async.NewAlert(
		alerts_async.Dispenser,
		alerts_async.WithEntity(&behavioralSpikeDestinationEntity{
			DestinationId:   dest.ID.String(),
			DestinationName: dest.Name,
			TenantId:        tenantId,
			DataPlaneId:     dest.DataPlaneId.String(),
		}),
		alerts_async.WithCriticality(alerts_async.Warning),
		alerts_async.WithFunctionalityType(alerts_async.VolumeAnomalyAlertChecker),
		alerts_async.WithTitle(title),
		alerts_async.WithMessage(message),
		alerts_async.WithErrorCode(alerts_async.DNDW10006, "Behavioral spike detected in delivery volume."),
		alerts_async.WithAction("Please investigate if this spike is expected. Check for: increased ingestion volumes, changes in volume control rules, transformation logic changes or destination configuration changes."),
	)
}

// behavioralSpikeDestinationEntity implements the entity interface for alerts
type behavioralSpikeDestinationEntity struct {
	DestinationId   string
	DestinationName string
	TenantId        string
	DataPlaneId     string
	alerts_async.NoSecondaryEntityId
}

func (e *behavioralSpikeDestinationEntity) GetEntityId() string {
	return e.DestinationId
}

func (e *behavioralSpikeDestinationEntity) GetEntityName() string {
	return e.DestinationName
}

func (e *behavioralSpikeDestinationEntity) GetTenantId() string {
	return e.TenantId
}

func (e *behavioralSpikeDestinationEntity) GetDataPlaneId() string {
	return e.DataPlaneId
}
