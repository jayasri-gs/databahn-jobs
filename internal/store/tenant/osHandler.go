package tenant

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"io"
	"strings"
)

var appConfig = config.GetAppConfiguration()
var _conf *os.OpenSearchConf
var client *opensearch.Client

func GetOsConf() *os.OpenSearchConf {
	if _conf == nil {
		_conf = os.GetConf()
	}
	return _conf
}

func GetIngestionByTenantId(ctx context.Context, startTime, endTime string) (map[string]any, map[string]any, error) {
	events, err := getTotalEventsIngestedByTenantId(ctx, startTime, endTime)
	if err != nil {
		return nil, nil, err
	}

	data, err := getTotalDataIngestedByTenantId(ctx, startTime, endTime)
	if err != nil {
		return nil, nil, err
	}

	return events, data, nil
}

func getTotalEventsIngestedByTenantId(ctx context.Context, startTime, endTime string) (map[string]any, error) {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	aggBy := "tags.db_tenant_id.keyword"
	client, err := getClient()
	if err != nil {
		return nil, err
	}
	return ExecuteAggQuery(ctx, client, q, aggBy, startTime, endTime)
}

func getTotalDataIngestedByTenantId(ctx context.Context, startTime, endTime string) (map[string]any, error) {
	q := `tags.component_name: "storage" AND name: "total_data_received"`
	aggBy := "tags.db_tenant_id.keyword"
	client, err := getClient()
	if err != nil {
		return nil, err
	}
	return ExecuteAggQuery(ctx, client, q, aggBy, startTime, endTime)
}

func getClient() (*opensearch.Client, error) {
	if client == nil {
		conf := os.GetConf()
		c, err := os.NewClient(context.Background(), conf.Url, conf.Creds())
		if err != nil {
			logger.GetLogger().Error("error while connecting to statistics store", zap.Error(err))
			return nil, err
		}
		client = c
	}
	return client, nil
}

func ExecuteAggQuery(ctx context.Context, client *opensearch.Client, q, aggBy, startTime, endTime string) (map[string]any, error) {
	conf := GetOsConf()
	query := statistics.AddDateRange(q, startTime, endTime)
	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(aggBy, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return nil, err
	}

	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while unmarshalling response", zap.Error(err))
		return nil, err
	}
	aggObj := statistics.NewAggregateResponse(resp)
	logger.GetLoggerWithContext(ctx).Info("got stats response for query", zap.Any("q", q))
	return aggObj.Agg, err
}
