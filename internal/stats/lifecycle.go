package stats

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/common"
	dbos "github.com/databahn-ai/databahn-jobs/internal/store/os"
	os "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
)

func RolloverLifecycle(ctx context.Context) common.JobResult {
	var jobErrors []common.JobError
	lifeCycleMigration := utils.GetEnvOrDefault("ROLLOVER_LIFECYCLE_MIGRATION", "")
	var indexLifeCycleMigration IndexLifeCycleMigration
	if lifeCycleMigration == string(Migrate_P1_P2) {
		indexLifeCycleMigration = Migrate_P1_P2
	} else if lifeCycleMigration == string(Migrate_P2_P3) {
		indexLifeCycleMigration = Migrate_P2_P3
	} else if lifeCycleMigration == string(Migrate_P3_P4) {
		indexLifeCycleMigration = Migrate_P3_P4
	} else {
		logger.GetLogger().Error("invalid lifecycle migration", zap.String("migration", lifeCycleMigration))
		errorMsg := fmt.Sprintf("invalid lifecycle migration: %s", lifeCycleMigration)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		return common.NewJobResultFromErrors(jobErrors)
	}
	config, err := parseConfig()
	if err != nil {
		errorMsg := fmt.Sprintf("error while parsing config: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while parsing config", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	logger.GetLogger().Info("starting stats lifecycle rollover:"+lifeCycleMigration, zap.Int("weeks_older_than", config.olderRolloverConfig.weeksOlderThan),
		zap.Int("parallelism", config.parallelism), zap.Int("limit", config.limit),
		zap.String("specific_index", config.olderRolloverConfig.specificIndex), zap.Any("skip_indices", config.olderRolloverConfig.skipIndices),
		zap.Int("agg_batch_size", config.aggBatchSize), zap.Any("agg_query_range", config.aggQueryRange),
		zap.Any("agg_window", config.aggWindow), zap.Any("validation_range", config.validationRange))

	indexNames, err := dbos.CatIndices(ctx, os.GetClient())
	if err != nil {
		errorMsg := fmt.Sprintf("error while fetching indices: %v", err)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("error while fetching indices", zap.Error(err))
		return common.NewJobResultFromErrors(jobErrors)
	}

	if indexLifeCycleMigration == Migrate_P1_P2 {
		indicesToRollover := filterStatsIndicesForP1Migration(indexNames)
		if len(indicesToRollover) == 0 {
			logger.GetLogger().Info("no indices to rollover for p1_p2")
			return common.NewJobResultSuccess()
		}
		logger.GetLogger().Info("indices to rollover for p1_p2", zap.Any("indices", indicesToRollover))
		config.aggWindow = 1 * time.Hour
		config.aggQueryRange = 1 * time.Hour
		config.validationRange = 1 * time.Hour
		rolloverErrors, successCount := runRolloverIndexToIndex(ctx, config, indicesToRollover, os.GetClient())
		if len(rolloverErrors) > 0 {
			return common.NewJobResultFromErrors(rolloverErrors)
		}
		logger.GetLogger().Info("successfully completed p1_p2 migration", zap.Int("success_count", successCount))
		return common.NewJobResultSuccess()
	} else if indexLifeCycleMigration == Migrate_P2_P3 {
		logger.GetLogger().Info("performing p2 to p3 migration")
		config.aggWindow = 24 * time.Hour
		config.aggQueryRange = 24 * time.Hour
		config.validationRange = 24 * time.Hour
		result := mergeP2Indices(ctx, config, indexNames, os.GetClient())
		if len(result.Errors) > 0 {
			return result
		}
		logger.GetLogger().Info("successfully completed p2_p3 migration")
		return common.NewJobResultSuccess()
	} else if indexLifeCycleMigration == Migrate_P3_P4 {
		logger.GetLogger().Info("performing p3 to p4 migration")
		config.aggWindow = 24 * time.Hour
		config.aggQueryRange = 24 * time.Hour
		config.validationRange = 24 * time.Hour
		result := mergeP3AndOlderRolledOverIndices(ctx, config, indexNames, os.GetClient())
		if len(result.Errors) > 0 {
			return result
		}
		logger.GetLogger().Info("successfully completed p3_p4 migration")
		return common.NewJobResultSuccess()
	}
	errorMsg := "invalid lifecycle migration, only p1_p2 and p2_p3 supported"
	jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
	return common.NewJobResultFromErrors(jobErrors)
}

func filterStatsIndicesForP1Migration(indexNames []string) []Index {
	var indicesToRollover []Index
	dayDiffConfig := utils.GetEnvInt("ROLLOVER_P1_P2_DAY_DIFFERENCE", 3)
	for _, index := range indexNames {
		if strings.HasPrefix(index, "db_statistics_") {
			indexObj, ok := parseIndexName(index)
			if ok {
				if shouldMigrateP1ToP2(*indexObj, dayDiffConfig) {
					indicesToRollover = append(indicesToRollover, *indexObj)
				}
			}
		}
	}
	sort.Slice(indicesToRollover, func(i, j int) bool {
		return yearDayNumber(indicesToRollover[i].Year, indicesToRollover[i].Day) < yearDayNumber(indicesToRollover[j].Year, indicesToRollover[j].Day)
	})
	return indicesToRollover
}

func shouldMigrateP1ToP2(index Index, dayDiffToConsiderForRollback int) bool {
	thisYear, thisDay := getYearAndDay()
	thisDayNumber := yearDayNumber(thisYear, thisDay)
	if index.Phase != Phase_P1 {
		return false
	}
	if index.Schema != Schema_V2 && index.Schema != Schema_V3_temp {
		return false
	}
	indexDayNumber := yearDayNumber(index.Year, index.Day)
	dayDifference := thisDayNumber - indexDayNumber
	return dayDifference > dayDiffToConsiderForRollback
}

func mergeP3AndOlderRolledOverIndices(ctx context.Context, config *RolloverConfig, allIndices []string, client *opensearch.Client) common.JobResult {
	var jobErrors []common.JobError
	weekDiff := utils.GetEnvInt("ROLLOVER_P3_P4_WEEK_DIFFERENCE", 15)
	year, week := getWeekOfYear()
	thisWeek := yearWeekNumber(year, week)
	var validRolledOverIndices []Index
	for _, indexName := range allIndices {
		if strings.HasPrefix(indexName, "rolled_over") {
			index, ok := parseRolledOverP3OrOlderIndexName(indexName)
			if ok {
				y := index.Year
				w := index.Week
				indexWeek := yearWeekNumber(y, w)
				if thisWeek-indexWeek > weekDiff {
					validRolledOverIndices = append(validRolledOverIndices, *index)
				}
			}
		}
	}
	if len(validRolledOverIndices) == 0 {
		logger.GetLogger().Info("no valid rolled over indices found for p3_p4 migration")
		return common.NewJobResultSuccess()
	}
	if config.specificTenants != "" {
		specificTenants := strings.Split(config.specificTenants, ",")
		var filteredRolledOverIndices []Index
		for _, index := range validRolledOverIndices {
			for _, tenant := range specificTenants {
				if index.Tenant == tenant {
					filteredRolledOverIndices = append(filteredRolledOverIndices, index)
					break
				}
			}
		}
		validRolledOverIndices = filteredRolledOverIndices
		if len(validRolledOverIndices) == 0 {
			logger.GetLogger().Info("no valid rolled over indices found for p3_p4 migration for specific tenants", zap.String("tenants", config.specificTenants))
			return common.NewJobResultSuccess()
		}
	}
	sort.Slice(validRolledOverIndices, func(i, j int) bool {
		return yearWeekNumber(validRolledOverIndices[i].Year, validRolledOverIndices[i].Week) < yearWeekNumber(validRolledOverIndices[j].Year, validRolledOverIndices[j].Week)
	})
	limit := config.limit
	if len(validRolledOverIndices) > limit {
		validRolledOverIndices = validRolledOverIndices[:limit]
	}
	logger.GetLogger().Info("found valid rolled over indices for p3_p4 migration", zap.Int("size", len(validRolledOverIndices)), zap.Any("indices", validRolledOverIndices))
	var validIndicesByTenant = make(map[string][]Index)
	for _, validIndex := range validRolledOverIndices {
		if _, ok := validIndicesByTenant[validIndex.Tenant]; !ok {
			validIndicesByTenant[validIndex.Tenant] = []Index{}
		}
		validIndicesByTenant[validIndex.Tenant] = append(validIndicesByTenant[validIndex.Tenant], validIndex)
	}
	for tenant, validIndicesOfTenant := range validIndicesByTenant {
		if len(validIndicesOfTenant) == 0 {
			logger.GetLogger().Info("no valid rolled over indices found for tenant", zap.String("tenant", tenant))
			continue
		}
		logger.GetLogger().Info("found valid rolled over indices for tenant", zap.String("tenant", tenant), zap.Int("size", len(validIndicesOfTenant)), zap.Any("indices", validIndicesOfTenant))
		var indicesByYear = make(map[int][]Index)
		for _, validIndex := range validIndicesOfTenant {
			if _, ok := indicesByYear[validIndex.Year]; !ok {
				indicesByYear[validIndex.Year] = []Index{}
			}
			indicesByYear[validIndex.Year] = append(indicesByYear[validIndex.Year], validIndex)
		}
		for indexYear, validIndicesOfYear := range indicesByYear {
			targetIndexName := fmt.Sprintf("rolled_over_1d_db_statistics_v2_p4_%s_y%d", tenant, indexYear)
			var successIndices []Index
			var rolloverError error = nil
			for _, validIndex := range validIndicesOfYear {
				logger.GetLogger().Info("staring rolled over index found for p3_p4 migration", zap.Any("index", validIndex))
				err := doRolloverAndValidate(ctx, validIndex, client, config, targetIndexName)
				if err != nil {
					logger.GetLogger().Error("error while rolling over index", zap.Error(err), zap.String("index", validIndex.Index))
					rolloverError = err
					break
				} else {
					successIndices = append(successIndices, validIndex)
				}
			}

			if len(successIndices) > 0 {
				aliasName := successIndices[0].aliasName()
				successIndicesNames := make([]string, len(successIndices))
				for i, index := range successIndices {
					successIndicesNames[i] = index.Index
				}
				err := dbos.UpdateMultipleAliases(client, aliasName, successIndicesNames, targetIndexName)
				if err != nil {
					errorMsg := fmt.Sprintf("error updating aliases for tenant %s, year %d: %v", tenant, year, err)
					jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
					logger.GetLogger().Error("error updating aliases", zap.Error(err), zap.String("tenant", tenant), zap.Int("year", year))
					return common.NewJobResultFromErrors(jobErrors)
				}
				logger.GetLogger().Info("rolled over and alias updated", zap.Any("older indices", successIndices), zap.String("rolled_over_index", targetIndexName))
				for _, index := range successIndices {
					err = dbos.DeleteIndex(ctx, client, index.Index)
					if err != nil {
						errorMsg := fmt.Sprintf("error deleting index %s for tenant %s, year %d: %v", index.Index, tenant, year, err)
						jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
						logger.GetLogger().Error("error deleting index", zap.Error(err), zap.String("index", index.Index), zap.String("tenant", tenant), zap.Int("year", year))
						return common.NewJobResultFromErrors(jobErrors)
					}
					logger.GetLogger().Info("deleted older index", zap.String("index", index.Index))
				}
			}
			if rolloverError != nil {
				errorMsg := fmt.Sprintf("rollover error for tenant %s, year %d: %v", tenant, year, rolloverError)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("rollover error", zap.Error(rolloverError), zap.String("tenant", tenant), zap.Int("year", year))
			}
			logger.GetLogger().Info("completed roll over index for indices", zap.String("tenantId", tenant), zap.Int("year", indexYear), zap.Any("indices", successIndices), zap.String("new_index", targetIndexName))
		}
	}

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("successfully completed stats lifecycle")
		return common.NewJobResultSuccess()
	} else {
		logger.GetLogger().Info("stats lifecycle completed with jobErrors", zap.Int("error_count", len(jobErrors)))
		return common.NewJobResultFromErrors(jobErrors)
	}
}

func mergeP2Indices(ctx context.Context, config *RolloverConfig, allIndices []string, client *opensearch.Client) common.JobResult {
	var jobErrors []common.JobError
	var rolledOverIndices []Index
	var nonRolledOverIndices []Index
	rolledOverIndicesByTenantAndWeek := make(map[string]map[int][]Index)
	nonRolledOverIndicesByTenantAndWeek := make(map[string]map[int][]Index)
	weekDiff := utils.GetEnvInt("ROLLOVER_P2_P3_WEEK_DIFFERENCE", 0)

	for _, indexName := range allIndices {
		if strings.HasPrefix(indexName, "rolled_over") {
			index, ok := parseRolledOverV2P2IndexName(indexName)
			if ok {
				rolledOverIndices = append(rolledOverIndices, *index)
			}
		} else if strings.HasPrefix(indexName, "db_statistics") {
			index, ok := parseIndexName(indexName)
			if ok {
				if index.Schema == Schema_V2 && index.Phase == Phase_P1 {
					nonRolledOverIndices = append(nonRolledOverIndices, *index)
				}
			}
		}
	}

	for _, index := range rolledOverIndices {
		if _, ok := rolledOverIndicesByTenantAndWeek[index.Tenant]; !ok {
			rolledOverIndicesByTenantAndWeek[index.Tenant] = make(map[int][]Index)
		}
		year, week := yearWeekFromYearDay(index.Year, index.Day)
		yearWeekId := yearWeekNumber(year, week)
		rolledOverIndicesByTenantAndWeek[index.Tenant][yearWeekId] = append(rolledOverIndicesByTenantAndWeek[index.Tenant][yearWeekId], index)
	}
	for _, index := range nonRolledOverIndices {
		if _, ok := nonRolledOverIndicesByTenantAndWeek[index.Tenant]; !ok {
			nonRolledOverIndicesByTenantAndWeek[index.Tenant] = make(map[int][]Index)
		}
		year, week := yearWeekFromYearDay(index.Year, index.Day)
		yearWeekId := yearWeekNumber(year, week)
		nonRolledOverIndicesByTenantAndWeek[index.Tenant][yearWeekId] = append(nonRolledOverIndicesByTenantAndWeek[index.Tenant][yearWeekId], index)
	}

	logger.GetLogger().Info("found valid rolled over indices", zap.Int("size", len(rolledOverIndices)), zap.Any("indices", rolledOverIndices))
	logger.GetLogger().Info("found valid non-rolled over indices", zap.Int("size", len(nonRolledOverIndices)), zap.Any("indices", nonRolledOverIndices))

	currentYear, currentDay := getYearAndDay()
	thisYear, thisWeek := yearWeekFromYearDay(currentYear, currentDay)
	thisWeekId := yearWeekNumber(thisYear, thisWeek)
	logger.GetLogger().Info("this week :", zap.Int("thisWeekId", thisWeekId))

	for tenant, theRolledOverIndicesByWeek := range rolledOverIndicesByTenantAndWeek {
		for weekId, theRolledOverIndices := range theRolledOverIndicesByWeek {
			if thisWeekId-weekId > weekDiff {
				nonRolledIndicesByWeek, ok := nonRolledOverIndicesByTenantAndWeek[tenant]
				if ok {
					nonRolledIndices, ok := nonRolledIndicesByWeek[weekId]
					if ok {
						if len(nonRolledIndices) > 0 {
							logger.GetLogger().Warn("there are non rolled over indices for rolled over week, not merging",
								zap.String("tenant", tenant), zap.Int("week", weekId), zap.Any("non_rolled_over_indices", nonRolledIndices))
							continue
						}
					}
				}
				if len(theRolledOverIndices) > 0 {
					newIndexYear, newIndexWeek := splitYearWeekNumber(weekId)
					logger.GetLogger().Info("will rollover p2-p3 indices", zap.Int("weekId", weekId),
						zap.String("tenant", tenant), zap.Any("rolledOverIndices", theRolledOverIndices))
					result := mergeP2IndicesIntoP3(ctx, config, tenant, theRolledOverIndices, newIndexYear, newIndexWeek, client)
					jobErrors = append(jobErrors, result.Errors...)
				}
			}
		}
	}
	if len(jobErrors) == 0 {
		return common.NewJobResultSuccess()
	}
	return common.NewJobResultFromErrors(jobErrors)
}

func mergeP2IndicesIntoP3(ctx context.Context, config *RolloverConfig, tenantId string, indicesToRollOver []Index, year, week int, client *opensearch.Client) common.JobResult {
	var jobErrors []common.JobError
	newIndexToMergeInto := fmt.Sprintf("rolled_over_1d_db_statistics_v2_p3_%s_y%d_w%d", tenantId, year, week)
	var successIndices []Index
	var rolloverError error = nil
	for _, index := range indicesToRollOver {
		err := doRolloverAndValidate(ctx, index, client, config, newIndexToMergeInto)
		if err != nil {
			logger.GetLogger().Error("error while rolling over index", zap.Error(err), zap.String("index", index.Index))
			rolloverError = err
			break
		} else {
			successIndices = append(successIndices, index)
		}
	}

	if len(successIndices) > 0 {
		var doneIndices []string
		for _, index := range successIndices {
			doneIndices = append(doneIndices, index.Index)
		}
		aliasName := successIndices[0].aliasName()
		err := dbos.UpdateMultipleAliases(client, aliasName, doneIndices, newIndexToMergeInto)
		if err != nil {
			errorMsg := fmt.Sprintf("error updating aliases: %v", err)
			jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
			logger.GetLogger().Error("error updating aliases", zap.Error(err))
			return common.NewJobResultFromErrors(jobErrors)
		}
		logger.GetLogger().Info("rolled over and alias updated", zap.Any("older indices", doneIndices), zap.String("rolled_over_index", newIndexToMergeInto))
		for _, index := range successIndices {
			err = dbos.DeleteIndex(ctx, client, index.Index)
			if err != nil {
				errorMsg := fmt.Sprintf("error deleting index %s: %v", index.Index, err)
				jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
				logger.GetLogger().Error("error deleting index", zap.Error(err), zap.String("index", index.Index))
				return common.NewJobResultFromErrors(jobErrors)
			}
			logger.GetLogger().Info("deleted older index", zap.String("index", index.Index))
		}
	}
	if rolloverError != nil {
		errorMsg := fmt.Sprintf("rollover error: %v", rolloverError)
		jobErrors = append(jobErrors, common.JobError{Message: errorMsg})
		logger.GetLogger().Error("rollover error", zap.Error(rolloverError))
	}

	logger.GetLogger().Info("completed roll over index for indices", zap.Any("indices", successIndices), zap.String("new_index", newIndexToMergeInto))
	if len(jobErrors) == 0 {
		return common.NewJobResultSuccess()
	}
	return common.NewJobResultFromErrors(jobErrors)
}
