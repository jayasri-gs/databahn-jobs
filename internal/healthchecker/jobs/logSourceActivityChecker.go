package jobs

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker"
	"github.com/databahn-ai/databahn-jobs/internal/healthchecker/helper"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/db-models/alerts_common"
	logSource "github.com/databahn-ai/db-models/log-source"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"io"
	"strconv"
	"strings"
	"time"
)

func getAggStatsForLogSource(ctx context.Context, startTime string, endTime string) (statistics.AggregateResponse, error) {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	agg := "tags.db_event_source_id.keyword"
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return statistics.AggregateResponse{}, err
	}

	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex, &searchBody, client)

	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return statistics.AggregateResponse{}, err
	}
	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewAggregateResponse(resp)
	logging.GetLoggerWithContext(ctx).Info("got response from statistics store")
	return aggObj, err
}

func AlertForLogSourceInactivity(ctx context.Context) error {
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logging.GetLogger())

	logging.GetLoggerWithContext(ctx).Info("Handling alerts for inactive logSources")

	//get agg stats by event source - returns all logsources which are reporting stats from last 15 minutes
	endTime := time.Now()
	startTime := endTime.Add(-time.Minute * healthchecker.LogSourceActivityCheckerTime)
	aggObj, err := getAggStatsForLogSource(ctx, strconv.Itoa(int(startTime.UnixMilli())), strconv.Itoa(int(endTime.UnixMilli())))
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting stats", zap.Error(err))
		return err
	}
	var lsIdArray []string
	for key, value := range aggObj.Agg {
		valueInt, ok := value.(float64)
		if !ok {
			logging.GetLoggerWithContext(ctx).Error("error while getting value of stats", zap.Error(err))
			return err
		}
		if valueInt > 0 {
			_, err := uuid.Parse(key)
			if err != nil {
				continue
			}
			lsIdArray = append(lsIdArray, key)
		}
	}
	//getting logSources which are active but did not report stats in last 15 minutes
	var activeLsArray []logSource.LogSource //array of ids not receiving stats
	err = config.GetDB().Model(&logSource.LogSource{}).Where("id not in ? and status = ?", lsIdArray, constants.StatusActive).Find(&activeLsArray).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while getting active logSources not receiving stats", zap.Error(err))
		return err
	}

	//creating alertEntityArray for all logSources for which alert needs to be raised
	var lsEntityArray []alerts_common.AlertEntityObject
	var silentLs []string
	for _, ls := range activeLsArray {
		var temp alerts_common.AlertEntityObject
		temp.EntityName = ls.Name
		temp.EntityId = ls.ID
		temp.EntityTenantUUId = ls.TenantUUID
		lsEntityArray = append(lsEntityArray, temp)

		silentLs = append(silentLs, ls.ID.String())
	}

	// update logsource mark silent
	err = config.GetDB().Model(&logSource.LogSource{}).Where("id in ? ", silentLs).Updates(map[string]interface{}{"reputation": common.SILENT}).Error
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while marking log sources as disabled", zap.Error(err))
		return err
	}

	// raise alert and save it to opensearch
	err = helper.SendAlertToControlFlag(ctx, lsEntityArray, alerts_common.LogSourceStatsNotReceivedTitle, alerts_common.LogSourceStatsNotReceivedMessage, alerts_common.LogSourceStatsNotReceived, alerts_common.LogSourceFunctionality, alerts_common.SevereAlert)
	if err != nil {
		logging.GetLoggerWithContext(ctx).Error("error while raid=sing alert for logSource activity check", zap.Error(err))
		return err
	}
	return nil
}
