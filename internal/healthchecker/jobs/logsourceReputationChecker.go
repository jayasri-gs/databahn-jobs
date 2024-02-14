package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	logSource "github.com/databahn-ai/db-models/log-source"
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
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered" and tags.db_event_source_id.keyword:` + lsId
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

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex, &searchBody, client)

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

	var ls []string
	err := config.GetDB().Model(&logSource.LogSource{}).Select("id").Find(&ls, "reputation != ?", common.SILENT).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting log sources", zap.Error(err))
		return err
	}

	endTime := time.Now()
	startTime := endTime.Add(-time.Hour * healthchecker.ReputationCheckerTime)
	startTimeThreshold := endTime.Add(-time.Hour * healthchecker.ReputationCheckerTimeThreshold)

	for _, lsId := range ls {
		thresholdAggObj, err := getHistogramForLogSource(ctx, strconv.Itoa(int(startTimeThreshold.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())), "1h", lsId)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
			return err
		}

		aggObj, err := getHistogramForLogSource(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())), "1h", lsId)
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
			return err
		}
		//silentThreshold, noisyThreshold := utils.CreateThresholds(thresholdAggObj.Buckets)

		// std deviation for stats for 6 hours duration
		mean := util.CalculateMean(convertBucketObjectToFloatArray(aggObj.Buckets))
		std := util.CalculateStandardDeviation(convertBucketObjectToFloatArray(aggObj.Buckets), mean)

		//  std deviation  for stats of threshold duration
		stdMean := util.CalculateMean(convertBucketObjectToFloatArray(thresholdAggObj.Buckets))
		stdThreshold := util.CalculateStandardDeviation(convertBucketObjectToFloatArray(thresholdAggObj.Buckets), stdMean)

		reputation := classifySources(std, stdThreshold)
		err = config.GetDB().Model(&logSource.LogSource{}).Where("id = ? ", lsId).Updates(map[string]interface{}{"reputation": reputation}).Error
		if err != nil {
			logging.GetLoggerWithContext(ctx).Error("error while marking log sources as disabled", zap.Error(err))
			return err
		}
	}
	return err
}

func classifySources(std, stdThreshold float64) int {
	if std < stdThreshold {
		return common.WHISPERING
	} else {
		return common.NOISY
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
