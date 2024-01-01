package insights

import (
	"context"
	"errors"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	"github.com/databahn-ai/databahn-jobs/internal/store"
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
	conf, err := configuration.NewAppConfig()
	if err != nil {
		return err
	}
	osClient, err := store.NewOpenSearchClient(ctx, conf)
	if err != nil {
		return err
	}
	lastWindowTime := time.Now().Add(-time.Minute * common.INSIGHTS_INTERVAL_MINUTES)
	lastTime, _ := util.FindWindow(lastWindowTime, time.Minute*common.INSIGHTS_INTERVAL_MINUTES)

	indexNames, err := store.CatIndices(ctx, osClient)
	if err != nil {
		return err
	}
	var allInsightsIndices []string
	for _, index := range indexNames {
		if strings.HasPrefix(index, common.INSIGHTS_STAGING_INDEX_PREFIX) {
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
		if index.IsBefore(window) {
			indicesToProcess = append(indicesToProcess, index)
		}
	}
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
				indexName := common.INSIGHTS_STAGING_INDEX_PREFIX + indexMetadata.String()
				if err != nil {
					errCount++
					logger.GetLogger().Error("failed to aggregate insights", zap.Error(err), zap.String("index", indexName))
				} else {
					successCount++
					logger.GetLogger().Info("successfully aggregated insights", zap.String("index", indexName))
					err := store.DeleteIndex(ctx, osClient, indexName)
					if err != nil {
						logger.GetLogger().Error("failed to delete index", zap.Error(err), zap.String("index", indexName))
					} else {
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
