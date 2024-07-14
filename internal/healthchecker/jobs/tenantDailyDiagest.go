package jobs

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/statistics"
	"github.com/databahn-ai/databahn-jobs/internal/store/tenant"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"io"
	"strconv"
	"strings"
	"time"
)

func TenantDailyDigest(ctx context.Context) error {
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	startTime := strconv.Itoa(int(time.Now().Add(-24 * time.Hour).UnixMilli()))
	endTime := strconv.Itoa(int(time.Now().UnixMilli()))

	t, err := tenant.GetTenants(ctx, config.GetDB())
	if err != nil {
		return err
	}
	eventsIngestedByTenantId, err := getTotalEventsIngested(ctx, client, startTime, endTime)
	if err != nil {
		logger.GetLogger().Error("error while getting total events ingested", zap.Error(err))
		return err
	}
	sizeIngestedByTenantId, err := getTotalDataReceived(ctx, client, startTime, endTime)
	if err != nil {
		logger.GetLogger().Error("error while getting total data received", zap.Error(err))
		return err
	}
	logger.GetLogger().Info("got total events ingested", zap.Reflect("eventsByTenantId", eventsIngestedByTenantId))
	for _, currTenant := range t {
		logger.GetLogger().Info("Processing tenant "+currTenant.Id.String(), zap.String("name", currTenant.Name))
		dailyDigest := tenant.Digest{
			TenantId: currTenant.Id,
			Name:     currTenant.Name,
		}

		if eventsIngestedByTenantId.Agg[currTenant.Id.String()] == nil {
			dailyDigest.TotalIngestionEvents = 0
			dailyDigest.IngestionHealth = "Unhealthy"
		} else {
			dailyDigest.TotalIngestionEvents = eventsIngestedByTenantId.Agg[currTenant.Id.String()].(float64)
			dailyDigest.IngestionHealth = "Healthy"
		}

		if sizeIngestedByTenantId.Agg[currTenant.Id.String()] == nil {
			dailyDigest.TotalIngestionSize = 0
		} else {
			dailyDigest.TotalIngestionSize = sizeIngestedByTenantId.Agg[currTenant.Id.String()].(float64)
		}

		logger.GetLogger().Info("daily digest for tenant", zap.String("tenantId", currTenant.Id.String()), zap.String("tenantName", currTenant.Name), zap.Reflect("digest", dailyDigest))
	}
	return nil
}

func getTotalEventsIngested(ctx context.Context, client *opensearch.Client, startTime, endTime string) (*statistics.AggregateResponse, error) {
	q := `tags.component_name: "ingestion" AND name: "total_events_delivered"`
	query := statistics.AddDateRange(q, startTime, endTime)
	agg := "tags.db_tenant_id.keyword"
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return nil, err
	}

	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return nil, err
	}

	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewAggregateResponse(resp)
	logger.GetLoggerWithContext(ctx).Info("got response from statistics store")
	return &aggObj, err
}

func getTotalDataReceived(ctx context.Context, client *opensearch.Client, startTime, endTime string) (*statistics.AggregateResponse, error) {
	q := `tags.component_name: "storage" AND name: "total_data_received"`
	query := statistics.AddDateRange(q, startTime, endTime)
	agg := "tags.db_tenant_id.keyword"
	conf := os.GetConf()
	client, err := os.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return nil, err
	}

	searchBody := &statistics.AggregateQueryRequest{}
	searchBody.Size = 0
	searchBody.Query.QueryString.Query = query

	aggList := strings.Split(agg, ",")
	searchBody.NestedAgg = statistics.BuildNextAggregation(aggList, 0)

	searchResponse, err := os.MakeSearchCall(ctx, conf.StatsIndex+"*", &searchBody, client)

	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while querying to statistics store", zap.Error(err), zap.String("url", conf.Url), zap.String("index", conf.StatsIndex))
		return nil, err
	}

	bodyContent, _ := io.ReadAll(searchResponse.Body)

	resp := &statistics.AggregateQueryResponse{}
	err = json.Unmarshal(bodyContent, resp)
	aggObj := statistics.NewAggregateResponse(resp)
	logger.GetLoggerWithContext(ctx).Info("got response from statistics store")
	return &aggObj, err
}
