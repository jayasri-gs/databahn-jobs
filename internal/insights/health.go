package insights

import (
	"context"
	"fmt"
	os "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"strings"
	"time"
)

const updateReputation = `
{
  "script": {
    "source": "
      if (ctx._source.reputation != params.skip_reputation) {
        ctx._source.reputation = params.reputation;
      }
      ctx._source.reputation_updated_at = params.updated_at;
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

func CalculateDeviceInventoryHealth(ctx context.Context, runningFor string) ([]HealthJobStatus, error) {
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics os", zap.Error(err))
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	indices, err := os.CatIndices(ctx, client)
	if err != nil {
		return nil, err
	}
	var sightsIndices []string
	var frequencyIndices []string
	for _, index := range indices {
		sightsIndexPrefix := fmt.Sprintf("%s%s_%s_", INSIGHTS_STORE_INDEX_PREFIX, APP_TYPE_SOURCEHOSTNAME, AGG_SIGHTS)
		if strings.HasPrefix(index, sightsIndexPrefix) {
			sightsIndices = append(sightsIndices, index)
		}
		frequencyIndexPrefix := fmt.Sprintf("%s%s_%s_", INSIGHTS_STORE_INDEX_PREFIX, APP_TYPE_SOURCEHOSTNAME, AGG_FREQUENCY)
		if strings.HasPrefix(index, frequencyIndexPrefix) {
			frequencyIndices = append(frequencyIndices, index)
		}
	}
	logger.GetLogger().Info("considering sights indices", zap.Int("index_count", len(sightsIndices)))
	logger.GetLogger().Info("considering frequency indices", zap.Int("index_count", len(frequencyIndices)))
	var statuses []HealthJobStatus

	for _, index := range sightsIndices {
		split := strings.Split(index, "_")
		tenantId := split[len(split)-1]
		_, err := uuid.Parse(tenantId)
		if err != nil {
			logger.GetLogger().Warn("failed to parse tenant id from sights index", zap.String("tenant_id", tenantId), zap.String("index", index), zap.Error(err))
			continue
		}
		statusHealth := HealthJobStatus{
			TenantId: tenantId,
			Action:   "SILENT_MARKING",
		}
		err = calculateDeviceInventoryHealthForTenant(ctx, client, tenantId, index, runningFor)
		if err != nil {
			logger.GetLogger().Error("failed to calculate silent device health for tenant", zap.String("tenant_id", tenantId), zap.Error(err))
			statusHealth.Status = STATUS_ERROR
			statusHealth.Error = err
		} else {
			statusHealth.Status = STATUS_SUCCESS
		}
		statuses = append(statuses, statusHealth)
	}

	for _, index := range frequencyIndices {
		split := strings.Split(index, "_")
		tenantId := split[len(split)-1]
		_, err := uuid.Parse(tenantId)
		if err != nil {
			logger.GetLogger().Warn("failed to parse tenant id from frequency index", zap.String("tenant_id", tenantId), zap.String("index", index), zap.Error(err))
			continue
		}

		statusNoise := HealthJobStatus{
			TenantId: tenantId,
			Action:   "NOISE_MARKING",
		}
		err = calculateNoiseOfDevices(ctx, client, tenantId, index, runningFor)
		if err != nil {
			logger.GetLogger().Error("failed to calculate noise of device for tenant", zap.String("tenant_id", tenantId), zap.Error(err))
			statusNoise.Status = STATUS_ERROR
			statusNoise.Error = err
		} else {
			statusNoise.Status = STATUS_SUCCESS
		}
		statuses = append(statuses, statusNoise)
	}

	return statuses, nil
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

func calculateNoiseOfDevices(ctx context.Context, client *opensearch.Client, tenantId string, frequencyIndexName string, runningFor string) error {
	beforeTime := noiseDate(runningFor)
	afterTime := time.Now().Add(-1 * time.Hour * 24 * NOISE_DAYS_TO_CONSIDER).UnixMilli()
	logger.GetLogger().Info("calculating device inventory noise for tenant", zap.String("tenant_id", tenantId),
		zap.String("index", frequencyIndexName), zap.Int64("before", beforeTime))

	query := fmt.Sprintf("day_end_timestamp:<=%d AND day_end_timestamp:>%d", beforeTime, afterTime)
	groupBy := []string{"key1.keyword", "key2.keyword", "source_id", "day_end_timestamp"}
	aggFuncName := "sum_count"
	functions := []os.AggregationFunction{{Function: "sum", Field: "count", Name: aggFuncName}}
	var after map[string]any
	var key1, key2, sourceId string
	var counts []float64
	var days []float64

	for {
		aggregate, newAfter, err := os.CompositePaginatedAggregate(ctx, client, INSIGHTS_READ_BATCH, frequencyIndexName, query, groupBy, functions, after)
		if err != nil {
			logger.GetLogger().Error("failed to paginate through aggregated device inventory frequency", zap.String("index", frequencyIndexName), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}
		after = newAfter
		if len(aggregate) == 0 {
			break
		}
		var requests []ReputationUpdateRequest
		for _, agg := range aggregate {
			tempKey1 := agg.Key["key1.keyword"].(string)
			tempKey2 := agg.Key["key2.keyword"].(string)
			tempSourceId := agg.Key["source_id"].(string)
			tempEodEpoch := agg.Key["day_end_timestamp"].(float64)
			tempCount := agg.Values[aggFuncName].(float64)

			if sourceId != "" && (tempKey1 != key1 || tempKey2 != key2 || tempSourceId != sourceId) {
				reputation, change, zScore := decideNoiseLevel(days, counts)
				logger.GetLogger().Info("decided noise level", zap.String("key1", key1), zap.String("key2", key2), zap.String("source_id", sourceId),
					zap.Float64("z_score", zScore), zap.String("reputation", reputation), zap.String("tenant_id", tenantId), zap.Bool("change", change))
				if change {
					request := ReputationUpdateRequest{
						Id:             InsightId(key1, key2, sourceId),
						Reputation:     reputation,
						SkipReputation: REPUTATION_SILENT,
						UpdatedAt:      time.Now().UnixMilli(),
					}
					requests = append(requests, request)
				}
				days = nil
				counts = nil
			}
			key1 = tempKey1
			key2 = tempKey2
			sourceId = tempSourceId
			days = append(days, tempEodEpoch)
			counts = append(counts, tempCount)
		}
		if len(requests) > 0 {
			sightIndexName := SightIndexName(tenantId)
			err = os.BulkUpsertWithScript(ctx, client, sightIndexName, updateReputation, requests, func(s ReputationUpdateRequest) string {
				return s.Id
			})
			if err != nil {
				logger.GetLogger().Error("failed to mark devices by noise level", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
				return err
			}
		}
	}

	//last batch if any
	var requests []ReputationUpdateRequest
	if sourceId != "" && key1 != "" {
		reputation, change, zScore := decideNoiseLevel(days, counts)
		logger.GetLogger().Info("decided noise level", zap.String("key1", key1), zap.String("key2", key2), zap.String("source_id", sourceId),
			zap.Float64("z_score", zScore), zap.String("reputation", reputation), zap.String("tenant_id", tenantId), zap.Bool("change", change))
		if change {
			request := ReputationUpdateRequest{
				Id:             InsightId(key1, key2, sourceId),
				Reputation:     reputation,
				SkipReputation: REPUTATION_SILENT,
				UpdatedAt:      time.Now().UnixMilli(),
			}
			requests = append(requests, request)
		}
	}

	if len(requests) > 0 {
		sightIndexName := SightIndexName(tenantId)
		err := os.BulkUpsertWithScript(ctx, client, sightIndexName, updateReputation, requests, func(s ReputationUpdateRequest) string {
			return s.Id
		})
		if err != nil {
			logger.GetLogger().Error("failed to mark devices by noise level", zap.String("index", sightIndexName), zap.String("tenant", tenantId), zap.Error(err))
			return err
		}
	}

	return nil
}

func decideNoiseLevel(days []float64, counts []float64) (string, bool, float64) {
	newDays, newCounts := sortTwoSlices(days, counts)
	lastDay := newDays[len(newDays)-1]
	lastCount := newCounts[len(newCounts)-1]
	if checkIfEodEpochIsYesterday(lastDay) {
		mean := util.CalculateMean(newCounts)
		deviation := util.CalculateStandardDeviation(newCounts, mean)
		zScore := util.CalculateZScore(lastCount, mean, deviation)
		if zScore > NOISE_ZSCORE_THRESHOLD {
			return REPUTATION_NOISY, true, zScore
		} else if zScore < (-1 * NOISE_ZSCORE_THRESHOLD) {
			return REPUTATION_WHISPERING, true, zScore
		} else {
			return "", false, zScore
		}
	}
	return "", false, 0
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
			history = append(history, sight.History(before, REPUTATION_SILENT))
			request := ReputationUpdateRequest{
				Id:             sight.Id,
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
