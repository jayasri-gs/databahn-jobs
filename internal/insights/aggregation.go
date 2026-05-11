package insights

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/store/source"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

func AggregateInsightsAndStore(ctx context.Context, parallelism int) JobResult {
	var errors []JobError

	lastWindowTime := time.Now().Add(-time.Minute * INSIGHTS_INTERVAL_MINUTES)
	lastTime, _ := util.FindWindow(lastWindowTime, time.Minute*INSIGHTS_INTERVAL_MINUTES)

	indexNames, err := os.CatIndices(ctx, os.GetClient())
	if err != nil {
		errors = append(errors, JobError{Message: fmt.Sprintf("failed to get indices: %v", err)})
		return NewJobResultFromErrors(errors)
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

	skipTenants := getSkipTenants()
	parquetTenants := getParquetTenants()
	var indicesByTenant = make(map[string][]IndexMetadata)
	for _, index := range indicesToProcess {
		if skipTenants[index.TenantId] {
			logger.GetLogger().Info("skipping tenant", zap.String("tenant_id", index.TenantId), zap.String("index", INSIGHTS_STAGING_INDEX_PREFIX+index.String()))
			continue
		}
		indicesByTenant[index.TenantId] = append(indicesByTenant[index.TenantId], index)
	}
	var logSources []source.Source
	err = config.GetDB().Model(&source.Source{}).Scan(&logSources).Error
	if err != nil {
		errors = append(errors, JobError{Message: fmt.Sprintf("failed to get log sources: %v", err)})
		return NewJobResultFromErrors(errors)
	}
	sourceIdToNameMap := make(map[string]string)
	for _, logSource := range logSources {
		sourceIdToNameMap[logSource.ID.String()] = logSource.Name
	}
	parrCtrl := make(chan struct{}, parallelism)
	successCount := 0
	var wg sync.WaitGroup
	var errorsMutex sync.Mutex

	for tenantId, tenantIndices := range indicesByTenant {
		wg.Add(1)
		parrCtrl <- struct{}{}
		func(tenantId string, indexMetadatas []IndexMetadata) {
			defer func() {
				<-parrCtrl
				wg.Done()
			}()
			for _, indexMetadata := range indexMetadatas {
				var w InsightsWriter
				if parquetTenants[tenantId] {
					w = &ParquetWriter{}
				} else {
					w = &JSONLWriter{}
				}
				err := aggregateInsights(ctx, os.GetClient(), indexMetadata, sourceIdToNameMap, w)
				indexName := INSIGHTS_STAGING_INDEX_PREFIX + indexMetadata.String()
				if err != nil {
					errorsMutex.Lock()
					errors = append(errors, JobError{Message: fmt.Sprintf("failed to aggregate insights for index %s: %v", indexName, err)})
					errorsMutex.Unlock()
					logger.GetLogger().Error("failed to aggregate insights", zap.Error(err), zap.String("index", indexName))
				} else {
					logger.GetLogger().Info("successfully aggregated insights", zap.String("index", indexName))
					err := os.DeleteIndex(ctx, os.GetClient(), indexName)
					if err != nil {
						errorsMutex.Lock()
						errors = append(errors, JobError{Message: fmt.Sprintf("failed to delete index %s: %v", indexName, err)})
						errorsMutex.Unlock()
						logger.GetLogger().Error("failed to delete index", zap.Error(err), zap.String("index", indexName))
					} else {
						successCount++
						logger.GetLogger().Info("successfully deleted index", zap.String("index", indexName))
					}
				}
			}

		}(tenantId, tenantIndices)
	}

	wg.Wait()
	if len(errors) > 0 {
		logger.GetLogger().Info("aggregation of insights done", zap.Int("index_count", len(indicesToProcess)), zap.Int("success_count", successCount), zap.Int("error_count", len(errors)))
		return NewJobResultFromErrors(errors)
	} else {
		logger.GetLogger().Info("successful aggregation of insights is done", zap.Int("index_count", len(indicesToProcess)), zap.Int("tenant_count", len(indicesByTenant)))
	}
	return NewJobResultSuccess()
}

func skipIndexTimeCheck() bool {
	return utils.GetEnvOrDefault("INSIGHTS_AGG_SKIP_INDEX_TIME_CHECK", "false") != "false"
}

func getParquetTenants() map[string]bool {
	val := utils.GetEnvOrDefault("INSIGHTS_PARQUET_TENANTS", "")
	result := make(map[string]bool)
	if val == "" {
		return result
	}
	for _, tenant := range strings.Split(val, ",") {
		t := strings.TrimSpace(tenant)
		if t != "" {
			result[t] = true
		}
	}
	return result
}

func getSkipTenants() map[string]bool {
	val := utils.GetEnvOrDefault("INSIGHTS_AGG_SKIP_TENANTS", "")
	result := make(map[string]bool)
	if val == "" {
		return result
	}
	for _, tenant := range strings.Split(val, ",") {
		t := strings.TrimSpace(tenant)
		if t != "" {
			result[t] = true
		}
	}
	return result
}
