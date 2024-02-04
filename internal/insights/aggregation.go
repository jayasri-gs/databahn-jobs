package insights

import (
	"context"
	"errors"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/store/opensearch"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func AggregateInsightsAndStore(ctx context.Context, parallelism int) error {
	conf := opensearch.GetConf()
	osClient, err := opensearch.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return err
	}
	if err != nil {
		return err
	}
	lastWindowTime := time.Now().Add(-time.Minute * INSIGHTS_INTERVAL_MINUTES)
	lastTime, _ := util.FindWindow(lastWindowTime, time.Minute*INSIGHTS_INTERVAL_MINUTES)

	indexNames, err := opensearch.CatIndices(ctx, osClient)
	if err != nil {
		return err
	}
	var allInsightsIndices []string
	for _, index := range indexNames {
		if strings.HasPrefix(index, INSIGHTS_STAGING_INDEX_PREFIX) {
			allInsightsIndices = append(allInsightsIndices, index)
		}
	}
	indices := parseIndices(allInsightsIndices)
	sort.Slice(indices, func(i, j int) bool {
		return indices[i].String() < indices[j].String()
	})

	var indicesToProcess []IndexMetadata
	window := time.UnixMilli(lastTime)
	for _, index := range indices {
		if skipIndexTimeCheck() || index.IsBefore(window) {
			indicesToProcess = append(indicesToProcess, index)
		}
	}

	logger.GetLogger().Info("calculated indices to process", zap.Int("index_count", len(indicesToProcess)), zap.Time("before", window))

	var indicesByTenant = make(map[string][]IndexMetadata)
	for _, index := range indicesToProcess {
		indicesByTenant[index.TenantId] = append(indicesByTenant[index.TenantId], index)
	}
	parrCtrl := make(chan struct{}, parallelism)
	errCount := 0
	successCount := 0
	var wg sync.WaitGroup
	for tenantId, tenantIndices := range indicesByTenant {
		wg.Add(1)
		parrCtrl <- struct{}{}
		func(tenantId string, indexMetadatas []IndexMetadata) {
			defer func() {
				<-parrCtrl
				wg.Done()
			}()
			for _, indexMetadata := range indexMetadatas {
				err := aggregateInsights(ctx, osClient, indexMetadata)
				indexName := INSIGHTS_STAGING_INDEX_PREFIX + indexMetadata.String()
				if err != nil {
					errCount++
					logger.GetLogger().Error("failed to aggregate insights", zap.Error(err), zap.String("index", indexName))
				} else {
					logger.GetLogger().Info("successfully aggregated insights", zap.String("index", indexName))
					err := opensearch.DeleteIndex(ctx, osClient, indexName)
					if err != nil {
						logger.GetLogger().Error("failed to delete index", zap.Error(err), zap.String("index", indexName))
						errCount++
					} else {
						successCount++
						logger.GetLogger().Info("successfully deleted index", zap.String("index", indexName))
					}
				}
			}

		}(tenantId, tenantIndices)
	}

	wg.Wait()
	if errCount > 0 {
		logger.GetLogger().Info("aggregation of insights done", zap.Int("index_count", len(indicesToProcess)), zap.Int("success_count", successCount), zap.Int("error_count", errCount))
		return errors.New(strconv.Itoa(errCount) + " failed to aggregate insights")
	} else {
		logger.GetLogger().Info("successful aggregation of insights is done", zap.Int("index_count", len(indicesToProcess)), zap.Int("tenant_count", len(indicesByTenant)))
	}
	return nil
}

func skipIndexTimeCheck() bool {
	return utils.GetEnvOrDefault("INSIGHTS_AGG_SKIP_INDEX_TIME_CHECK", "false") != "false"
}
