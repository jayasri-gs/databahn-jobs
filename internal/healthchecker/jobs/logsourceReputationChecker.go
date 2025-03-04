package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/db-models/alerts_common"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"io"
	"strconv"
	"time"
)

type Source struct {
	TenantId string
	SourceId string
	Type     int
	Data     []float64
}

func getHistogramForLogSource(ctx context.Context, startTime string, endTime string, interval string, lsId string) (statistics.HistogramResponse, error) {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered" AND tags.db_event_source_id.keyword:` + lsId
	query := statistics.AddDateRange(q, startTime, endTime)
	if interval == "" {
		return statistics.HistogramResponse{}, errors.New("interval is required")
	}
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return statistics.HistogramResponse{}, err
	}

	searchBody := &statistics.HistogramQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query
	searchBody.Aggs.SumOverTime.DateHistogram.Field = statistics.ES_TIME_FIELD
	searchBody.Aggs.SumOverTime.DateHistogram.Interval = interval
	searchBody.Aggs.SumOverTime.Aggs.SumValue.Sum.Field = statistics.ES_COUNTER_VALUE_FIELD

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return statistics.HistogramResponse{}, err
	}
	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.HistogramQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewHistogramResponse(resp)
	return aggObj, err
}

func UpdateReputationForLogSources(ctx context.Context) error {
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("getting histogram all log sources")

	var logSources []source.Source
	err := config.GetDB().Model(&source.Source{}).Scan(&logSources).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting log sources", zap.Error(err))
		return err
	}

	configLogSources, err := source.GetConfigLogSourceIds(ctx)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while fetching config log source ids", zap.Error(err))
		return err
	}

	logging.GetLogger().Info("config log sources", zap.Any("configlogSources", configLogSources))

	endTime := time.Now()
	startTime := endTime.Add(-time.Hour * time.Duration(util.GetEnvInt64FromString(healthchecker.ReputationCheckerTime)))
	startTimeThreshold := endTime.Add(-time.Hour * time.Duration(util.GetEnvInt64FromString(healthchecker.ReputationCheckerTimeThreshold)))

	var whisperingAlertsEntityArray, noisyAlertsEntityArray []alerts_common.AlertBaseObjectV2
	var whisperingLs, noisyLs []string
	for _, ls := range logSources {
		thresholdAggObj, err := getHistogramForLogSource(ctx, strconv.Itoa(int(startTimeThreshold.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())), "1h", ls.ID.String())
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
			return err
		}

		aggObj, err := getHistogramForLogSource(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())), "1h", ls.ID.String())
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
			return err
		}
		//silentThreshold, noisyThreshold := utils.CreateThresholds(thresholdAggObj.Buckets)

		// std deviation for stats for 6 hours duration
		mean := util.CalculateMean(convertBucketObjectToFloatArray(aggObj.Buckets))
		//std := util.CalculateStandardDeviation(convertBucketObjectToFloatArray(aggObj.Buckets), mean)

		//  std deviation  for stats of threshold duration
		meanThreshold := util.CalculateMean(convertBucketObjectToFloatArray(thresholdAggObj.Buckets))
		stdThreshold := util.CalculateStandardDeviation(convertBucketObjectToFloatArray(thresholdAggObj.Buckets), meanThreshold)

		zScoreMean := util.CalculateZScore(mean, meanThreshold, stdThreshold)
		reputation := classifySources(zScoreMean)

		if reputation == common.WHISPERING {
			var temp alerts_common.AlertBaseObjectV2
			temp.EntityName = ls.Name
			temp.EntityId = ls.ID
			temp.EntityTenantUUId = ls.TenantID
			temp.DataPlaneId = ls.DataPlaneId
			temp.AlertType = alerts_common.AlertTypeExternalAndExternal
			temp.ErrorCode = healthchecker.DNDE10001
			whisperingAlertsEntityArray = append(whisperingAlertsEntityArray, temp)
			whisperingLs = append(whisperingLs, ls.ID.String())
		} else if reputation == common.NOISY {
			var temp alerts_common.AlertBaseObjectV2
			temp.EntityName = ls.Name
			temp.EntityId = ls.ID
			temp.EntityTenantUUId = ls.TenantID
			temp.DataPlaneId = ls.DataPlaneId
			temp.AlertType = alerts_common.AlertTypeExternalAndExternal
			temp.ErrorCode = healthchecker.DNDE10001
			noisyAlertsEntityArray = append(noisyAlertsEntityArray, temp)
			noisyLs = append(noisyLs, ls.ID.String())
		}
	}
	err = markReputation(ctx, noisyLs, whisperingLs)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while marking log sources", zap.Error(err))
		return err
	}

	if len(configLogSources) > 0 {
		var filteredWhisperingAlertsEntityArray, filteredNoisyAlertsEntityArray []alerts_common.AlertBaseObjectV2
		for _, alert := range whisperingAlertsEntityArray {
			for _, configLogSource := range configLogSources {
				if alert.EntityTenantUUId.String() == configLogSource.TenantID && alert.EntityId.String() == configLogSource.SourceID {
					logging.GetLoggerWithContext(ctx).Info("whispering log source found in config", zap.String("tenantId", configLogSource.TenantID), zap.String("sourceId", configLogSource.SourceID))
					filteredWhisperingAlertsEntityArray = append(filteredWhisperingAlertsEntityArray, alert)
				}
			}
		}
		for _, alert := range noisyAlertsEntityArray {
			for _, configLogSource := range configLogSources {
				if alert.EntityTenantUUId.String() == configLogSource.TenantID && alert.EntityId.String() == configLogSource.SourceID {
					logging.GetLoggerWithContext(ctx).Info("noisy log source found in config", zap.String("tenantId", configLogSource.TenantID), zap.String("sourceId", configLogSource.SourceID))
					filteredNoisyAlertsEntityArray = append(filteredNoisyAlertsEntityArray, alert)
				}
			}
		}
		if len(filteredNoisyAlertsEntityArray) > 0 {
			noisyAlertsEntityArray = filteredNoisyAlertsEntityArray
		}

		if len(filteredWhisperingAlertsEntityArray) > 0 {
			whisperingAlertsEntityArray = filteredWhisperingAlertsEntityArray
		}

	}

	err = raiseAlerts(ctx, noisyAlertsEntityArray, whisperingAlertsEntityArray)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while raising alerts", zap.Error(err))
		return err
	}

	return err
}

func markReputation(ctx context.Context, noisyLs []string, whisperingLs []string) error {
	// mark reputation for whispering
	err := config.GetDB().Model(&source.Source{}).Where("id in ? ", whisperingLs).Updates(map[string]interface{}{"reputation": common.WHISPERING}).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while marking log sources as whispering", zap.Error(err))
		return err
	}

	// mark reputation for noisy
	err = config.GetDB().Model(&source.Source{}).Where("id in ? ", noisyLs).Updates(map[string]interface{}{"reputation": common.NOISY}).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while marking log sources as noisy", zap.Error(err))
		return err
	}

	return nil
}

func raiseAlerts(ctx context.Context, noisyAlertsEntityArray []alerts_common.AlertBaseObjectV2, whisperingAlertsEntityArray []alerts_common.AlertBaseObjectV2) error {
	// raise alert for whispering
	if len(whisperingAlertsEntityArray) > 0 {
		err := helper.SendAlertToControlPlane(ctx, whisperingAlertsEntityArray, alerts_common.WhisperingAlertTitle, alerts_common.WhisperingAlertMessage, alerts_common.WhisperingAlertType, alerts_common.LogSourceFunctionality, alerts_common.WarningAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while raising alerts for whispering log sources", zap.Error(err))
			return err
		}
	}

	// raise alert for noisy log sources
	if len(noisyAlertsEntityArray) > 0 {
		err := helper.SendAlertToControlPlane(ctx, noisyAlertsEntityArray, alerts_common.NoisyAlertTitle, alerts_common.NoisyAlertMessage, alerts_common.NoisyAlertType, alerts_common.LogSourceFunctionality, alerts_common.WarningAlert, alerts_common.AlertOpen, false, "system")
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while raising alerts for noisy log sources", zap.Error(err))
			return err
		}
	}

	return nil
}

func classifySources(zScoreMean float64) string {
	if zScoreMean > common.NoisyThreshold {
		return common.NOISY
	} else if zScoreMean < (-1 * common.NoisyThreshold) {
		return common.WHISPERING
	} else {
		return common.STABLE
	}
}

// convertBucketObjectToFloatArray
func convertBucketObjectToFloatArray(buckets []statistics.HistogramBucket) []float64 {
	var data []float64
	for _, value := range buckets {
		data = append(data, value.Value)
	}
	return data
}
