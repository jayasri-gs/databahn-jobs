package insights

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/common"
	appConfig "github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/athena"
	os "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/synapse"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
)

const updateReputation = `
{
  "script": {
    "source": "
      if (ctx._source.reputation != params.skip_reputation) {
        ctx._source.reputation = params.reputation;
      }
      ctx._source.updated_at = params.updated_at;
    ",
    "lang": "painless",
    "params": {
      "reputation": "{{.Reputation}}",
      "skip_reputation": "{{.SkipReputation}}",
      "updated_at": {{.UpdatedAt}}
    }
  }
}
`

type HealthJobStatus struct {
	TenantId string
	Action   string
	Status   string
	Error    error
}

func CalculateDeviceInventoryHealth(ctx context.Context, runningFor string) common.JobResult {
	var jobErrors []common.JobError

	indices, err := os.CatIndices(ctx, os.GetClient())
	if err != nil {
		errorMsg := fmt.Sprintf("error fetching indices: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error fetching indices", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}
	var sightsIndices []string
	for _, index := range indices {
		sightsIndexPrefix := fmt.Sprintf("%s%s_%s_", INSIGHTS_STORE_INDEX_PREFIX, AGG_SIGHTS, APP_TYPE_SOURCEHOSTNAME)
		if strings.HasPrefix(index, sightsIndexPrefix) {
			sightsIndices = append(sightsIndices, index)
		}
	}
	logger.GetLogger().Info("considering sights indices for device health calculation", zap.Int("index_count", len(sightsIndices)))

	var statuses []HealthJobStatus

	// Process each tenant: mark silent devices and calculate noise levels
	for _, index := range sightsIndices {
		split := strings.Split(index, "_")
		tenantId := split[len(split)-1]
		_, err := uuid.Parse(tenantId)
		if err != nil {
			logger.GetLogger().Warn("failed to parse tenant id from sights index", zap.String("tenant_id", tenantId), zap.String("index", index), zap.Error(err))
			continue
		}

		// Silent marking based on last seen time
		statusHealth := HealthJobStatus{
			TenantId: tenantId,
			Action:   "SILENT_MARKING",
		}
		err = calculateDeviceInventoryHealthForTenant(ctx, os.GetClient(), tenantId, index, runningFor)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to calculate silent device health for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("failed to calculate silent device health for tenant", zap.String("tenant_id", tenantId), zap.Error(err))
			statusHealth.Status = STATUS_ERROR
			statusHealth.Error = err
		} else {
			statusHealth.Status = STATUS_SUCCESS
		}
		statuses = append(statuses, statusHealth)

		// Noise marking based on frequency data from S3/Athena
		statusNoise := HealthJobStatus{
			TenantId: tenantId,
			Action:   "NOISE_MARKING",
		}
		// Note: calculateNoiseOfDevices now queries S3 via Athena (no longer uses OpenSearch frequency index)
		err = calculateNoiseOfDevices(ctx, os.GetClient(), tenantId, runningFor)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to calculate noise of device for tenant %s: %v", tenantId, err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("failed to calculate noise of device for tenant", zap.String("tenant_id", tenantId), zap.Error(err))
			statusNoise.Status = STATUS_ERROR
			statusNoise.Error = err
		} else {
			statusNoise.Status = STATUS_SUCCESS
		}
		statuses = append(statuses, statusNoise)
	}

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("successfully completed device inventory health calculation", zap.Int("status_count", len(statuses)))
		return common.NewJobResultSuccess()
	} else {
		logger.GetLogger().Info("device inventory health calculation completed with errors", zap.Int("error_count", len(jobErrors)), zap.Int("status_count", len(statuses)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}

func calculateDeviceInventoryHealthForTenant(ctx context.Context, client *opensearch.Client, tenantId string, sightIndexName string, runningFor string) error {
	before := silenceDate(runningFor)
	logger.GetLogger().Info("calculating device inventory health for tenant", zap.String("tenant_id", tenantId),
		zap.String("index", sightIndexName), zap.Int64("before", before))

	err := markDevicesSilent(ctx, client, tenantId, sightIndexName, before)
	if err != nil {
		logger.GetLogger().Error("failed to mark devices silent", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
		return err
	}

	err = markDevicesUnSilent(ctx, client, tenantId, sightIndexName, before)
	if err != nil {
		logger.GetLogger().Error("failed to mark devices non silent", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
		return err
	}

	return nil
}

func calculateNoiseOfDevices(ctx context.Context, client *opensearch.Client, tenantId string, runningFor string) error {
	beforeTime := noiseDate(runningFor)
	endDate := time.UnixMilli(beforeTime)
	startDate := time.Now().Add(-1 * time.Hour * 24 * NOISE_DAYS_TO_CONSIDER)

	searchBackend := appConfig.GetAppConfiguration().GetString(SearchBackendKey)
	var aggregates []athena.FrequencyAggregation
	var err error
	if IsSynapseBackend(searchBackend) {
		logger.GetLogger().Info("calculating device inventory noise for tenant using Azure Synapse",
			zap.String("tenant_id", tenantId),
			zap.String("start_date", startDate.Format("2006-01-02")),
			zap.String("end_date", endDate.Format("2006-01-02")),
			zap.Int("days_to_consider", NOISE_DAYS_TO_CONSIDER))
		aggregates, err = synapse.QueryFrequencyData(ctx, tenantId, startDate, endDate)
		if err != nil {
			logger.GetLogger().Error("failed to query frequency data from Synapse",
				zap.String("tenant_id", tenantId), zap.Error(err))
			return err
		}
		logger.GetLogger().Info("received frequency aggregations from Synapse",
			zap.String("tenant_id", tenantId),
			zap.Int("total_records", len(aggregates)))
	} else {
		logger.GetLogger().Info("calculating device inventory noise for tenant using Athena",
			zap.String("tenant_id", tenantId),
			zap.String("start_date", startDate.Format("2006-01-02")),
			zap.String("end_date", endDate.Format("2006-01-02")),
			zap.Int("days_to_consider", NOISE_DAYS_TO_CONSIDER))
		aggregates, err = athena.QueryFrequencyData(ctx, tenantId, startDate, endDate)
		if err != nil {
			logger.GetLogger().Error("failed to query frequency data from Athena",
				zap.String("tenant_id", tenantId), zap.Error(err))
			return err
		}
		logger.GetLogger().Info("received frequency aggregations from Athena",
			zap.String("tenant_id", tenantId),
			zap.Int("total_records", len(aggregates)))
	}

	// Process aggregates - group by device (key1, key2, source_id)
	var key1, key2, sourceId string
	var counts []float64
	var days []float64
	var requests []ReputationUpdateRequest
	var history []SilentDeviceHistory

	for _, agg := range aggregates {
		// Check if we've moved to a new device
		if sourceId != "" && (agg.Key1 != key1 || agg.Key2 != key2 || agg.SourceId != sourceId) {
			// Process the previous device
			logger.GetLogger().Debug("processing device for noise calculation",
				zap.String("key1", key1),
				zap.String("key2", key2),
				zap.String("source_id", sourceId),
				zap.Int("days_accumulated", len(days)),
				zap.Float64s("day_timestamps", days),
				zap.Float64s("day_counts", counts))

			reputation, change, zScore, metadata := decideNoiseLevel(days, counts)
			logger.GetLogger().Info("decided noise level",
				zap.String("key1", key1),
				zap.String("key2", key2),
				zap.String("source_id", sourceId),
				zap.Float64("z_score", zScore),
				zap.String("reputation", reputation),
				zap.String("tenant_id", tenantId),
				zap.Bool("change", change))

			if change {
				request := ReputationUpdateRequest{
					Id:             InsightId(key1, key2, "", "", "", sourceId),
					Key1:           key1,
					Key2:           key2,
					SourceId:       sourceId,
					TenantId:       tenantId,
					Type:           APP_TYPE_SOURCEHOSTNAME,
					Reputation:     reputation,
					SkipReputation: REPUTATION_SILENT,
					UpdatedAt:      time.Now().UnixMilli(),
				}
				requests = append(requests, request)

				// Create history record
				if len(days) > 0 {
					lastDay := int64(days[len(days)-1])
					sight := Sight{
						Id:          InsightId(key1, key2, "", "", "", sourceId),
						Key1:        key1,
						Key2:        key2,
						Type:        APP_TYPE_SOURCEHOSTNAME,
						SourceId:    sourceId,
						TenantId:    tenantId,
						DataPlaneId: "", // Not available in Athena query results
					}
					history = append(history, sight.History(lastDay, reputation, metadata))
				}
			}

			// Reset for new device
			days = nil
			counts = nil
		}

		// Accumulate data for current device
		key1 = agg.Key1
		key2 = agg.Key2
		sourceId = agg.SourceId
		days = append(days, float64(agg.DayEndTimestamp))
		counts = append(counts, agg.Count)
	}

	// Process last device if any
	if sourceId != "" && key1 != "" {
		reputation, change, zScore, metadata := decideNoiseLevel(days, counts)
		logger.GetLogger().Info("decided noise level (last)",
			zap.String("key1", key1),
			zap.String("key2", key2),
			zap.String("source_id", sourceId),
			zap.Float64("z_score", zScore),
			zap.String("reputation", reputation),
			zap.String("tenant_id", tenantId),
			zap.Bool("change", change))

		if change {
			request := ReputationUpdateRequest{
				Id:             InsightId(key1, key2, "", "", "", sourceId),
				Key1:           key1,
				Key2:           key2,
				SourceId:       sourceId,
				TenantId:       tenantId,
				Type:           APP_TYPE_SOURCEHOSTNAME,
				Reputation:     reputation,
				SkipReputation: REPUTATION_SILENT,
				UpdatedAt:      time.Now().UnixMilli(),
			}
			requests = append(requests, request)

			// Create history record
			if len(days) > 0 {
				lastDay := int64(days[len(days)-1])
				sight := Sight{
					Id:          InsightId(key1, key2, "", "", "", sourceId),
					Key1:        key1,
					Key2:        key2,
					Type:        APP_TYPE_SOURCEHOSTNAME,
					SourceId:    sourceId,
					TenantId:    tenantId,
					DataPlaneId: "", // Not available in Athena query results
				}
				history = append(history, sight.History(lastDay, reputation, metadata))
			}
		}
	}

	// Update/Insert (upsert) reputations in OpenSearch sights index
	if len(requests) > 0 {
		sightIndexName := SightIndexName(tenantId)
		logger.GetLogger().Info("upserting device reputations in OpenSearch",
			zap.String("tenant_id", tenantId),
			zap.String("index", sightIndexName),
			zap.Int("upsert_count", len(requests)))

		err := os.BulkUpsertWithScript(ctx, client, sightIndexName, updateReputation, requests, func(s ReputationUpdateRequest) string {
			return s.Id
		})
		if err != nil {
			logger.GetLogger().Error("failed to upsert devices by noise level",
				zap.String("index", sightIndexName),
				zap.String("tenant", tenantId),
				zap.Error(err))
			return err
		}

		logger.GetLogger().Info("successfully upserted device reputations",
			zap.String("tenant_id", tenantId),
			zap.Int("upserted_count", len(requests)))

		// Save history records with reputation metadata
		if len(history) > 0 {
			historyIndexName := SilentDeviceInventoryHistoryIndex(tenantId)
			logger.GetLogger().Info("saving reputation history records",
				zap.String("tenant_id", tenantId),
				zap.String("index", historyIndexName),
				zap.Int("history_count", len(history)))

			err := os.BulkUpsert(ctx, client, historyIndexName, history, func(h SilentDeviceHistory) string {
				return h.Id
			})
			if err != nil {
				logger.GetLogger().Error("failed to save reputation history",
					zap.String("index", historyIndexName),
					zap.String("tenant", tenantId),
					zap.Error(err))
				return err
			}

			logger.GetLogger().Info("successfully saved reputation history",
				zap.String("tenant_id", tenantId),
				zap.Int("history_count", len(history)))
		}
	} else {
		logger.GetLogger().Info("no reputation changes needed",
			zap.String("tenant_id", tenantId))
	}

	return nil
}

func decideNoiseLevel(days []float64, counts []float64) (string, bool, float64, *ReputationMetadata) {
	logger.GetLogger().Debug("decideNoiseLevel called",
		zap.Int("days_count", len(days)),
		zap.Int("counts_count", len(counts)),
		zap.Float64s("days", days),
		zap.Float64s("counts", counts))

	if len(counts) < 3 {
		logger.GetLogger().Debug("insufficient data for noise calculation",
			zap.Int("count", len(counts)),
			zap.Int("required", 3))
		return "", false, 0, nil
	}

	newDays, newCounts := sortTwoSlices(days, counts)
	lastDay := newDays[len(newDays)-1]
	lastCount := newCounts[len(newCounts)-1]
	sampleCounts := newCounts[:len(newCounts)-1] // BUG FIX: was using unsorted counts

	logger.GetLogger().Debug("sorted data for noise calculation",
		zap.Float64s("sorted_days", newDays),
		zap.Float64s("sorted_counts", newCounts),
		zap.Float64("last_day", lastDay),
		zap.String("last_day_date", time.UnixMilli(int64(lastDay)).Format("2006-01-02")),
		zap.Float64("last_count", lastCount),
		zap.Float64s("sample_counts", sampleCounts))

	isYesterday := checkIfEodEpochIsYesterday(lastDay)

	logger.GetLogger().Info("checking if last day is yesterday",
		zap.Float64("last_day_timestamp", lastDay),
		zap.String("last_day_date", time.UnixMilli(int64(lastDay)).Format("2006-01-02")),
		zap.String("yesterday_date", time.Now().Add(-24*time.Hour).Format("2006-01-02")),
		zap.Bool("is_yesterday", isYesterday))

	if isYesterday {
		mean := util.CalculateMean(sampleCounts)
		deviation := util.CalculateStandardDeviation(sampleCounts, mean)

		logger.GetLogger().Info("noise level statistics",
			zap.Float64("mean", mean),
			zap.Float64("std_dev", deviation),
			zap.Float64("last_count", lastCount),
			zap.Int("sample_size", len(sampleCounts)))

		if deviation == 0 || math.IsNaN(deviation) {
			logger.GetLogger().Debug("zero or NaN deviation, skipping",
				zap.Float64("deviation", deviation))
			return "", false, 0, nil
		}

		zScore := util.CalculateZScore(lastCount, mean, deviation)
		threshold := dynamicThreshold(len(sampleCounts))

		logger.GetLogger().Info("z-score calculation",
			zap.Float64("z_score", zScore),
			zap.Float64("threshold", threshold))

		// Create historic data map
		historicData := make(map[string]float64)
		for i := 0; i < len(sampleCounts); i++ {
			dateStr := time.UnixMilli(int64(newDays[i])).Format("2006-01-02")
			historicData[dateStr] = newCounts[i]
		}

		// Calculate expected range based on threshold
		expectedMin := mean - (threshold * deviation)
		expectedMax := mean + (threshold * deviation)
		if expectedMin < 0 {
			expectedMin = 0 // Event counts can't be negative
		}

		var reputation string
		var reason string
		if zScore > threshold {
			reputation = REPUTATION_NOISY
			reason = fmt.Sprintf("Device sent %.0f events, which is significantly above the expected range of %.0f-%.0f events (based on historical average of %.0f events)",
				lastCount, expectedMin, expectedMax, mean)
		} else if zScore < (-1 * threshold) {
			reputation = REPUTATION_WHISPERING
			reason = fmt.Sprintf("Device sent %.0f events, which is significantly below the expected range of %.0f-%.0f events (based on historical average of %.0f events)",
				lastCount, expectedMin, expectedMax, mean)
		}

		if reputation != "" {
			metadata := &ReputationMetadata{
				Mean:             mean,
				StandardDev:      deviation,
				ZScore:           zScore,
				SampleSize:       len(sampleCounts),
				Threshold:        threshold,
				ActualCount:      lastCount,
				ExpectedRangeMin: expectedMin,
				ExpectedRangeMax: expectedMax,
				HistoricData:     historicData,
				Reason:           reason,
				CalculatedAt:     time.Now().UnixMilli(),
			}
			return reputation, true, zScore, metadata
		}
		return "", false, zScore, nil
	} else {
		logger.GetLogger().Debug("last day is not yesterday, skipping",
			zap.Float64("last_day", lastDay),
			zap.String("last_day_date", time.UnixMilli(int64(lastDay)).Format("2006-01-02")),
			zap.String("yesterday", time.Now().Add(-24*time.Hour).Format("2006-01-02")))
	}
	return "", false, 0, nil
}

func dynamicThreshold(sampleSize int) float64 {
	if sampleSize < 10 {
		return 5.0
	} else if sampleSize < 30 {
		return 3.0
	} else {
		return 1.5
	}
}

func sortTwoSlices(days []float64, counts []float64) ([]float64, []float64) {
	for i := 0; i < len(days); i++ {
		for j := i + 1; j < len(days); j++ {
			if days[i] > days[j] {
				days[i], days[j] = days[j], days[i]
				counts[i], counts[j] = counts[j], counts[i]
			}
		}
	}
	return days, counts
}

func markDevicesSilent(ctx context.Context, client *opensearch.Client, tenantId string, sightIndexName string, before int64) error {
	query := fmt.Sprintf("max_time:<%d", before)
	sort := []os.Sort{{
		Field: "key1.keyword",
		Order: "asc",
	}, {
		Field: "key2.keyword",
		Order: "asc",
	},
		{
			Field: "source_id",
			Order: "asc",
		}}
	var after []any
	silentDevicesCount := 0
	for {
		data, newAfter, err := os.SearchPaginated(ctx, client, sightIndexName, query, INSIGHTS_READ_BATCH, after, sort)
		if err != nil {
			logger.GetLogger().Error("failed to paginate through device inventory sights", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}
		after = newAfter
		if len(data) == 0 {
			break
		}
		var sights []Sight
		decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &sights})
		err = decoder.Decode(data)
		if err != nil {
			logger.GetLogger().Error("failed to decode device inventory sights", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}
		var history []SilentDeviceHistory
		var silentRequests []ReputationUpdateRequest
		for _, sight := range sights {
			// Create metadata for SILENT reputation
			metadata := &ReputationMetadata{
				LastEventSeenMs: sight.MaxTime,
				Reason:          fmt.Sprintf("Device has not sent any events since %s (threshold: %d days)", time.UnixMilli(sight.MaxTime).Format("2006-01-02 15:04:05"), SilentDaysBefore),
				CalculatedAt:    time.Now().UnixMilli(),
			}
			history = append(history, sight.History(before, REPUTATION_SILENT, metadata))
			request := ReputationUpdateRequest{
				Id:             sight.Id,
				Key1:           sight.Key1,
				Key2:           sight.Key2,
				SourceId:       sight.SourceId,
				TenantId:       sight.TenantId,
				Type:           sight.Type,
				Reputation:     REPUTATION_SILENT,
				SkipReputation: REPUTATION_SILENT,
				UpdatedAt:      time.Now().UnixMilli(),
			}
			silentRequests = append(silentRequests, request)
		}

		err = os.BulkUpsertWithScript(ctx, client, sightIndexName, updateReputation, silentRequests, func(s ReputationUpdateRequest) string {
			return s.Id
		})
		if err != nil {
			logger.GetLogger().Error("failed to mark devices silent", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}

		silentHistoryIndex := SilentDeviceInventoryHistoryIndex(tenantId)
		err = os.BulkUpsert(ctx, client, silentHistoryIndex, history, func(h SilentDeviceHistory) string {
			return h.Id
		})
		if err != nil {
			logger.GetLogger().Error("failed to upsert silent device history", zap.String("index", silentHistoryIndex), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}

		silentDevicesCount += len(silentRequests)
	}
	logger.GetLogger().Info("marked devices silent", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Int("count", silentDevicesCount))
	return nil
}

func markDevicesUnSilent(ctx context.Context, client *opensearch.Client, tenantId string, sightIndexName string, before int64) error {
	query := fmt.Sprintf("max_time:>%d AND reputation:%s", before, REPUTATION_SILENT)
	sort := []os.Sort{{
		Field: "key1.keyword",
		Order: "asc",
	}, {
		Field: "key2.keyword",
		Order: "asc",
	},
		{
			Field: "source_id",
			Order: "asc",
		}}
	var after []any
	unSilentCount := 0
	for {
		data, newAfter, err := os.SearchPaginated(ctx, client, sightIndexName, query, INSIGHTS_READ_BATCH, after, sort)
		if err != nil {
			logger.GetLogger().Error("failed to paginate through device inventory sights", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}
		after = newAfter
		if len(data) == 0 {
			break
		}
		var sights []Sight
		err = mapstructure.Decode(data, &sights)
		if err != nil {
			logger.GetLogger().Error("failed to decode device inventory sights", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}
		var silentRequests []ReputationUpdateRequest
		for _, sight := range sights {
			request := ReputationUpdateRequest{
				Id:             sight.Id,
				Key1:           sight.Key1,
				Key2:           sight.Key2,
				SourceId:       sight.SourceId,
				TenantId:       sight.TenantId,
				Type:           sight.Type,
				Reputation:     REPUTATION_NORMAL,
				SkipReputation: REPUTATION_NORMAL,
				UpdatedAt:      time.Now().UnixMilli(),
			}
			silentRequests = append(silentRequests, request)
		}

		err = os.BulkUpsertWithScript(ctx, client, sightIndexName, updateReputation, silentRequests, func(s ReputationUpdateRequest) string {
			return s.Id
		})
		if err != nil {
			logger.GetLogger().Error("failed to mark devices normal", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}
		unSilentCount += len(silentRequests)
	}
	logger.GetLogger().Info("marked devices normal from silent", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Int("count", unSilentCount))
	return nil
}

func silenceDate(runningFor string) int64 {
	reduceDays := 1
	if runningFor == HEALTH_CALCULATION_YESTERDAY {
		reduceDays = 2
	}
	date := time.Now().Add(-1 * time.Hour * 24 * time.Duration(reduceDays))
	return util.GetDayEndTimestamp(date.UnixMilli())
}

func noiseDate(runningFor string) int64 {
	reduceDays := 1
	date := time.Now().Add(-1 * time.Hour * 24 * time.Duration(reduceDays))
	return util.GetDayEndTimestamp(date.UnixMilli())
}

func checkIfEodEpochIsYesterday(eodEpoch float64) bool {
	ep := int64(eodEpoch)
	yesterday := time.Now().Add(-1 * time.Hour * 24)
	yesterdayEod := util.GetDayEndTimestamp(yesterday.UnixMilli())
	return ep == yesterdayEod
}
