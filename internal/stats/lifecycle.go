package stats

import (
	"context"
	"errors"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	dbos "github.com/databahn-ai/databahn-jobs/internal/store/os"
	os "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"go.uber.org/zap"
	"sort"
	"strings"
	"time"
)

func RolloverLifecycle(ctx context.Context) error {
	lifeCycleMigration := utils.GetEnvOrDefault("ROLLOVER_LIFECYCLE_MIGRATION", "")
	var indexLifeCycleMigration IndexLifeCycleMigration
	if lifeCycleMigration == string(Migrate_P1_P2) {
		indexLifeCycleMigration = Migrate_P1_P2
	} else if lifeCycleMigration == string(Migrate_P2_P3) {
		indexLifeCycleMigration = Migrate_P2_P3
	} else {
		logger.GetLogger().Error("invalid lifecycle migration", zap.String("migration", lifeCycleMigration))
		return errors.New("invalid lifecycle migration")
	}
	config, err := parseConfig()
	if err != nil {
		logger.GetLogger().Error("error while parsing config", zap.Error(err))
		return err
	}

	logger.GetLogger().Info("starting stats lifecycle rollover:"+lifeCycleMigration, zap.Int("weeks_older_than", config.olderRolloverConfig.weeksOlderThan),
		zap.Int("parallelism", config.parallelism), zap.Int("limit", config.limit),
		zap.String("specific_index", config.olderRolloverConfig.specificIndex), zap.Any("skip_indices", config.olderRolloverConfig.skipIndices),
		zap.Int("agg_batch_size", config.aggBatchSize), zap.Any("agg_query_range", config.aggQueryRange),
		zap.Any("agg_window", config.aggWindow), zap.Any("validation_range", config.validationRange))

	indexNames, err := dbos.CatIndices(ctx, os.GetClient())
	if err != nil {
		logger.GetLogger().Error("error while fetching indices", zap.Error(err))
		return err
	}

	if indexLifeCycleMigration == Migrate_P1_P2 {
		indicesToRollover := filterStatsIndicesForP1Migration(indexNames)
		if len(indicesToRollover) == 0 {
			logger.GetLogger().Info("no indices to rollover for p1_p2")
			return nil
		}
		logger.GetLogger().Info("indices to rollover for p1_p2", zap.Any("indices", indicesToRollover))
		config.aggWindow = 1 * time.Hour
		config.aggQueryRange = 1 * time.Hour
		config.validationRange = 1 * time.Hour
		return runRolloverIndexToIndex(ctx, config, indicesToRollover, os.GetClient())
	} else if indexLifeCycleMigration == Migrate_P2_P3 {
		logger.GetLogger().Info("performing p2 to p3 migration")
		config.aggWindow = 24 * time.Hour
		config.aggQueryRange = 24 * time.Hour
		config.validationRange = 24 * time.Hour
		return mergeP2Indices(ctx, config, indexNames, os.GetClient())
	}
	return errors.New("invalid lifecycle migration, only p1_p2 and p2_p3 supported")
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
	if index.Schema != Schema_V2 || index.Phase != Phase_P1 {
		return false
	}
	indexDayNumber := yearDayNumber(index.Year, index.Day)
	dayDifference := thisDayNumber - indexDayNumber
	return dayDifference > dayDiffToConsiderForRollback
}

func mergeP2Indices(ctx context.Context, config *RolloverConfig, allIndices []string, client *opensearch.Client) error {
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
					err := mergeP2IndicesIntoP3(ctx, config, tenant, theRolledOverIndices, newIndexYear, newIndexWeek, client)
					if err != nil {
						logger.GetLogger().Error("error while merging p2 indices into p3", zap.Error(err), zap.Any("indices", theRolledOverIndices))
						return err
					}
				}
			}
		}
	}
	return nil
}

func mergeP2IndicesIntoP3(ctx context.Context, config *RolloverConfig, tenantId string, indicesToRollOver []Index, year, week int, client *opensearch.Client) error {
	newIndexToMergeInto := fmt.Sprintf("rolled_over_1d_db_statistics_v2_p3_%s_y%d_w%d", tenantId, year, week)
	for _, index := range indicesToRollOver {
		err := doRolloverAndValidate(ctx, index, client, config, newIndexToMergeInto)
		if err != nil {
			logger.GetLogger().Error("error while rolling over index", zap.Error(err), zap.String("index", index.Index))
		}
	}

	var olderIndices []string
	for _, index := range indicesToRollOver {
		olderIndices = append(olderIndices, index.Index)
	}
	aliasName := indicesToRollOver[0].aliasName()
	err := dbos.UpdateMultipleAliases(client, aliasName, olderIndices, newIndexToMergeInto)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("rolled over and alias updated", zap.Any("older indices", olderIndices), zap.String("rolled_over_index", newIndexToMergeInto))
	for _, index := range indicesToRollOver {
		err = dbos.DeleteIndex(ctx, client, index.Index)
		if err != nil {
			return err
		}
		logger.GetLogger().Info("deleted older index", zap.String("index", index.Index))
	}

	logger.GetLogger().Info("completed roll over index for indices", zap.Any("indices", indicesToRollOver), zap.String("new_index", newIndexToMergeInto))
	return nil
}
