package stats

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	dbos "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func RolloverOlderStats(ctx context.Context) error {
	config, err := parseConfig()
	if err != nil {
		logger.GetLogger().Error("error while parsing config", zap.Error(err))
		return err
	}

	logger.GetLogger().Info("starting stats rollover", zap.Int("weeks_older_than", config.olderRolloverConfig.weeksOlderThan),
		zap.Int("parallelism", config.parallelism), zap.Int("limit", config.limit),
		zap.String("specific_index", config.olderRolloverConfig.specificIndex), zap.Any("skip_indices", config.olderRolloverConfig.skipIndices),
		zap.Int("agg_batch_size", config.aggBatchSize), zap.Any("agg_query_range", config.aggQueryRange),
		zap.Any("agg_window", config.aggWindow), zap.Any("validation_range", config.validationRange))

	var indexNames []string

	if config.olderRolloverConfig.specificIndex == "" {
		indexNames, err = dbos.CatIndices(ctx, dbos.GetClient())
		if err != nil {
			logger.GetLogger().Error("error while fetching indices", zap.Error(err))
			return err
		}
	} else {
		indices := strings.Split(config.olderRolloverConfig.specificIndex, ",")
		indexNames = indices
	}

	indicesToRollover := filterStatsValidIndices(indexNames, config.olderRolloverConfig.weeksOlderThan, config.limit, config.olderRolloverConfig.skipIndices)
	logger.GetLogger().Info("indices to rollover", zap.Any("indices", indicesToRollover))

	return runRolloverIndexToIndex(ctx, config, indicesToRollover, dbos.GetClient())
}

func runRolloverIndexToIndex(ctx context.Context, config *RolloverConfig, indicesToRollover []Index, osClient *opensearch.Client) error {
	errrCount := 0
	successCount := 0
	wg := sync.WaitGroup{}
	parallelismCntrl := make(chan struct{}, config.parallelism)
	for i, index := range indicesToRollover {
		wg.Add(1)
		parallelismCntrl <- struct{}{}
		indexCopy := index
		go func(j int, anIndex Index) {
			defer func() {
				wg.Done()
				<-parallelismCntrl
			}()
			err := rollover(ctx, anIndex, osClient, config)
			if err != nil {
				errrCount++
				logger.GetLogger().Error("error while rolling over index "+anIndex.Index, zap.Error(err), zap.Int("index_number", j))
			} else {
				successCount++
			}
		}(i, indexCopy)
	}
	wg.Wait()
	logger.GetLogger().Info("rolled over stats indices", zap.Int("success_count", successCount), zap.Int("error_count", errrCount))
	if errrCount == 0 {
		return nil
	} else {
		return fmt.Errorf("%d errors while rolling over indices", errrCount)
	}
}

func parseConfig() (*RolloverConfig, error) {
	weeksOlderThan := utils.GetEnvInt("STATS_ROLLOVER_OLDER_THAN_WEEKS", 2)
	parallelism := utils.GetEnvInt("STATS_ROLLOVER_PARALLELISM", 4)
	limit := utils.GetEnvInt("STATS_ROLLOVER_INDEX_LIMIT", 10)
	specificIndex := utils.GetEnvOrDefault("STATS_ROLLOVER_SPECIFIC_INDEX", "")
	skipIndices := utils.GetEnvOrDefault("STATS_ROLLOVER_SKIP_INDICES", "")
	aggBatchSize := utils.GetEnvInt("STATS_ROLLOVER_AGG_BATCH_SIZE", 500)
	aggQueryRange := utils.GetEnvOrDefault("STATS_ROLLOVER_AGG_QUERY_RANGE", "3h")
	validationRange := utils.GetEnvOrDefault("STATS_ROLLOVER_VALIDATION_RANGE", "24h")
	aggWindow := utils.GetEnvOrDefault("STATS_ROLLOVER_AGG_WINDOW", "1m")
	s3BackupEnabled := strings.EqualFold(utils.GetEnvOrDefault("STATS_ROLLOVER_S3_BACKUP_ENABLED", "true"), "true")
	deleteExistingRolledOverIndex := strings.EqualFold(utils.GetEnvOrDefault("STATS_ROLLOVER_DELETE_EXISTING_ROLLED_OVER_INDEX", "false"), "true")
	skipValidation := strings.EqualFold(utils.GetEnvOrDefault("STATS_ROLLOVER_SKIP_VALIDATION", "false"), "true")

	aggQueryDuration, err := time.ParseDuration(aggQueryRange)
	if err != nil {
		logger.GetLogger().Error("error while parsing agg query range", zap.Error(err), zap.String("range", aggQueryRange))
		return nil, err
	}
	aggWindowDuration, err := time.ParseDuration(aggWindow)
	if err != nil {
		logger.GetLogger().Error("error while parsing agg window", zap.Error(err), zap.String("window", aggWindow))
		return nil, err
	}
	validationRangeDuration, err := time.ParseDuration(validationRange)
	if err != nil {
		logger.GetLogger().Error("error while parsing validation window", zap.Error(err), zap.String("window", validationRange))
		return nil, err
	}
	if aggQueryDuration <= aggWindowDuration {
		logger.GetLogger().Error("agg query range should be greater than agg window", zap.String("range", aggQueryRange), zap.String("window", aggWindow))
		return nil, errors.New("agg query range should be greater than agg window")
	}

	if validationRangeDuration < aggWindowDuration {
		logger.GetLogger().Error("validation range should be greater than agg window", zap.String("validation", validationRange), zap.String("window", aggWindow))
		return nil, errors.New("validation range should be greater than agg window")
	}
	skips := strings.Split(skipIndices, ",")
	olderConf := OlderRolloverConfig{
		weeksOlderThan: weeksOlderThan,
		specificIndex:  specificIndex,
		skipIndices:    skips,
	}
	rolloverConf := RolloverConfig{
		olderRolloverConfig:           &olderConf,
		parallelism:                   parallelism,
		limit:                         limit,
		aggBatchSize:                  aggBatchSize,
		aggQueryRange:                 aggQueryDuration,
		validationRange:               validationRangeDuration,
		aggWindow:                     aggWindowDuration,
		s3BackupEnabled:               s3BackupEnabled,
		deleteExistingRolledOverIndex: deleteExistingRolledOverIndex,
		skipValidation:                skipValidation,
	}
	return &rolloverConf, nil
}

func splitByTimeRanges(minEpoch, maxEpoch int64, duration time.Duration) []timeRange {
	var ranges []timeRange
	startTime := time.UnixMilli(minEpoch).UTC()
	endTime := time.UnixMilli(maxEpoch).UTC()
	if endTime.Sub(startTime) <= duration {
		ranges = append(ranges, timeRange{start: startTime.UnixMilli(), end: endTime.UnixMilli()})
		ranges[len(ranges)-1].end = ranges[len(ranges)-1].end + 1
		return ranges
	}
	startOfRange := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, time.UTC)
	for startOfRange.Before(endTime) {
		endOfRange := startOfRange.Add(duration)
		ranges = append(ranges, timeRange{start: startOfRange.UnixMilli(), end: endOfRange.UnixMilli()})
		startOfRange = endOfRange
	}
	ranges[len(ranges)-1].end = ranges[len(ranges)-1].end + 1
	return ranges
}

func filterStatsValidIndices(indexNames []string, weeksOlderThan int, limit int, skip []string) []Index {
	thisYear, thisWeek := getWeekOfYear()
	thisYearWeekNumber := yearWeekNumber(thisYear, thisWeek)
	var indicesToRollover []Index
	for _, index := range indexNames {
		if strings.HasPrefix(index, "db_statistics_") {
			indexObj, ok := parseIndexName(index)
			if ok && indexObj.Schema == Schema_V1 {
				yearWeek := yearWeekNumber(indexObj.Year, indexObj.Week)
				if yearWeek < thisYearWeekNumber-weeksOlderThan && !contains(skip, index) {
					indicesToRollover = append(indicesToRollover, *indexObj)
				}
			}
		}
	}
	sort.Slice(indicesToRollover, func(i, j int) bool {
		return yearWeekNumber(indicesToRollover[i].Year, indicesToRollover[i].Week) < yearWeekNumber(indicesToRollover[j].Year, indicesToRollover[j].Week)
	})
	if len(indicesToRollover) > limit {
		return indicesToRollover[:limit]
	}
	return indicesToRollover
}

func rollover(ctx context.Context, index Index, client *opensearch.Client, config *RolloverConfig) error {
	newIndexName, err := index.newIndexNameForRollover(config.aggWindow)
	if err != nil {
		logger.GetLogger().Error("error while building new index name", zap.Error(err), zap.String("index", index.Index))
		return err
	}
	err = doRolloverAndValidate(ctx, index, client, config, newIndexName)
	if err != nil {
		return err
	}

	logger.GetLogger().Info("rolled over and index validated", zap.String("index", index.Index), zap.String("rolled_over_index", newIndexName))
	if config.s3BackupEnabled {
		err = uploadOlderStatsToS3(ctx, index, client)
		if err != nil {
			return err
		}
		logger.GetLogger().Info("rolled over original backed up", zap.String("index", index.Index), zap.String("rolled_over_index", newIndexName))
	} else {
		logger.GetLogger().Info("s3 backup disabled", zap.String("index", index.Index))
	}
	err = updateAlias(index, client, newIndexName)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("rolled over alias updated", zap.String("index", index.Index), zap.String("rolled_over_index", newIndexName))
	err = dbos.DeleteIndex(ctx, client, index.Index)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("deleted older index", zap.String("index", index.Index))
	logger.GetLogger().Info("completed roll over index " + index.Index)
	return nil
}

func doRolloverAndValidate(ctx context.Context, index Index, client *opensearch.Client, config *RolloverConfig, newIndexName string) error {
	logger.GetLoggerWithContext(ctx).Info("rolling over index", zap.String("index", index.Index))
	minVal, maxVal, err := findMinMaxTimestamp(ctx, index, client)
	if err != nil {
		return err
	}
	timeRanges := splitByTimeRanges(minVal, maxVal, config.aggQueryRange)
	newIndexValues := 0
	if err != nil {
		return err
	}

	if config.deleteExistingRolledOverIndex {
		err = dbos.DeleteIndex(ctx, client, newIndexName)
		if err != nil {
			return err
		}
		logger.GetLogger().Info("deleted existing rolled over index", zap.String("index", newIndexName))
	}

	for _, tr := range timeRanges {
		start := tr.start
		end := tr.end
		var after *After
		for {
			rolloverRequest := buildRolloverAggRequest(start, end, after, config.aggBatchSize, config.aggWindow)
			response, err := makeSearchCallAndParseResponse(ctx, index, rolloverRequest, client)
			if err != nil {
				return err
			}

			if len(response.Aggregations.CompositeBuckets.Buckets) == 0 {
				logger.GetLogger().Debug("no more documents to process", zap.String("index", index.Index))
				break
			}
			after = response.Aggregations.CompositeBuckets.AfterKey
			var sources []EsSource
			for _, compositeBucket := range response.Aggregations.CompositeBuckets.Buckets {
				timeHistogramBucket := compositeBucket.Key.TimeHistogramBuckets
				newDocId := compositeBucket.Key.newDocKey(index.Index)
				sampleValues := compositeBucket.AllFields.Hits.Hits
				if len(sampleValues) == 0 {
					logger.GetLogger().Error("no AllFields found for agg key", zap.Any("key", compositeBucket.Key), zap.String("index", index.Index))
					return errors.New("no AllFields found for agg key")
				}
				sampleValue := sampleValues[0]
				newSource := sampleValue.Source
				newSource.Id = newDocId
				counterSum := compositeBucket.TotalCount.Value
				counterValue, err := parseCounter(counterSum)
				if err != nil {
					return err
				}
				newSource.Counter.Value = counterValue
				newSource.Timestamp = timeHistogramBucket
				newSource.Tags["db_ts_win"] = timeHistogramBucket
				sources = append(sources, newSource)
				newIndexValues++
			}
			err = insertIntoNewIndex(ctx, sources, client, newIndexName)
			if err != nil {
				return err
			}
		}
	}
	if !config.skipValidation {
		err = validateNewData(ctx, index, client, newIndexName, minVal, maxVal, config.validationRange)
		if err != nil {
			return err
		}
	} else {
		logger.GetLogger().Info("skipping validation", zap.String("index", index.Index))
	}
	return nil
}

func parseCounter(counterSum any) (float64, error) {
	counterStr, ok := counterSum.(string)
	if ok {
		counterFloat, err := strconv.ParseFloat(counterStr, 64)
		if err != nil {
			return 0, err
		}
		return counterFloat, nil
	} else {
		counterFloat, ok := counterSum.(float64)
		if ok {
			return counterFloat, nil
		}
		return 0, fmt.Errorf("counter value %v not a float or string", counterSum)
	}
}

func findMinMaxTimestamp(ctx context.Context, index Index, client *opensearch.Client) (int64, int64, error) {
	req := MinMaxRequest{}
	req.Size = 0
	req.Aggs.MinVal.Min.Field = "tags.db_ts_win"
	req.Aggs.MaxVal.Max.Field = "tags.db_ts_win"
	resp, err := dbos.MakeSearchCall(ctx, index.Index, req, client)
	if err != nil {
		return 0, 0, err
	}
	response, err := parseMinMaxResponse(resp)
	if err != nil {
		return 0, 0, err
	}
	minVal := int64(response.Aggregations.MinVal.Value)
	maxVal := int64(response.Aggregations.MaxVal.Value)
	return minVal, maxVal, nil
}

func parseMinMaxResponse(resp *opensearchapi.Response) (*MinMaxResponse, error) {
	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New("failed to get successful response from opensearch for min max, body:" + string(bodyContent))
	}
	response := MinMaxResponse{}
	err = json.Unmarshal(bodyContent, &response)
	if err != nil {
		return nil, err
	}
	if response.Error != nil && response.Error.Reason != "" {
		return nil, errors.New(response.Error.Reason)
	}
	return &response, nil
}

func makeSearchCallAndParseResponse(ctx context.Context, index Index, rolloverRequest RolloverAggRequest, client *opensearch.Client) (*RolloverAggResponse, error) {
	resp, err := dbos.MakeSearchCall(ctx, index.Index, rolloverRequest, client)
	if err != nil {
		return nil, err
	}
	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, errors.New("failed to get successful response from opensearch, body:" + string(bodyContent))
	}
	response := RolloverAggResponse{}
	err = json.Unmarshal(bodyContent, &response)
	if err != nil {
		return nil, err
	}
	if response.Error != nil && response.Error.Reason != "" {
		return nil, errors.New(response.Error.Reason)
	}
	return &response, nil
}

func updateAlias(index Index, client *opensearch.Client, newIndexName string) error {
	alias := index.aliasName()
	return dbos.UpdateAliases(client, alias, index.Index, newIndexName)
}

func uploadOlderStatsToS3(ctx context.Context, index Index, client *opensearch.Client) error {
	var after []any
	sort := []dbos.Sort{
		dbos.Sort{
			Field: "document_id.keyword",
			Order: "asc",
		},
	}
	i := 0
	pageSize := 20
	var zipWriter *gzip.Writer
	var zipFile *os.File
	fileHadPendingData := false
	start := time.Now()
	for {
		fileSuffix := strconv.Itoa(i / pageSize)
		fileName := index.s3FileName(fileSuffix)
		if zipFile == nil && zipWriter == nil {
			f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			zipFile = f
			zipWriter = gzip.NewWriter(zipFile)
		}
		data, newAfter, err := dbos.SearchPaginated(ctx, client, index.Index, "*", 5000, after, sort)
		if err != nil {
			return err
		}
		after = newAfter
		if len(data) != 0 {
			err = writeToBackupFile(zipWriter, data)
			fileHadPendingData = true
			if err != nil {
				return err
			}
			if (i+1)%pageSize == 0 {
				end := time.Now()
				logger.GetLogger().Info("processed page", zap.String("index", index.Index), zap.Int("page", i/pageSize), zap.Duration("duration", end.Sub(start)))
				err = upload(ctx, index, zipFile, zipWriter)
				if err != nil {
					return err
				}
				zipWriter = nil
				zipFile = nil
				fileHadPendingData = false
				start = time.Now()
			}
		} else {
			break
		}
		i++
	}
	if zipFile != nil && fileHadPendingData {
		end := time.Now()
		logger.GetLogger().Info("processed last page", zap.String("index", index.Index), zap.Duration("duration", end.Sub(start)))
		err := upload(ctx, index, zipFile, zipWriter)
		if err != nil {
			return err
		}
	}
	return nil
}

func upload(ctx context.Context, index Index, s3File *os.File, writer *gzip.Writer) error {
	start := time.Now()
	defer func() {
		end := time.Now()
		uploadDur := end.Sub(start)
		logger.GetLogger().Info("uploaded file to s3", zap.String("index", index.Index), zap.Duration("upload_duration", uploadDur))
	}()
	fileBaseName := filepath.Base(s3File.Name())
	err := writer.Close()
	if err != nil {
		return err
	}
	err = s3File.Close()
	if err != nil {
		return err
	}
	objectKey := fmt.Sprintf("stats_backup/tenant_id=%s/year=%d/week=%d/%s", index.Tenant, index.Year, index.Week, fileBaseName)
	err = util.UploadGzipFileToS3(ctx, objectKey, s3File.Name())
	if err != nil {
		return err
	}
	return nil
}

func writeToBackupFile(writer *gzip.Writer, docs []map[string]any) error {
	for _, doc := range docs {
		j, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		if _, err := writer.Write(j); err != nil {
			return err
		}
		if _, err := writer.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

func validateNewData(ctx context.Context, index Index, client *opensearch.Client, newIndexName string,
	minVal, maxVal int64, validationDuration time.Duration) error {
	err := dbos.RefreshIndex(ctx, client, newIndexName)
	if err != nil {
		return err
	}

	olderIndexGroupBy := []string{"tags.db_tenant_id.keyword", "tags.db_event_source_id.keyword", "name.raw", "namespace"}
	aggregations := []dbos.AggregationFunction{
		dbos.AggregationFunction{
			Name:     "total_count",
			Field:    "counter.value",
			Function: "sum",
		},
	}

	validationRanges := splitByTimeRanges(minVal, maxVal, validationDuration)
	for _, vr := range validationRanges {
		query := fmt.Sprintf("tags.db_ts_win:[%d TO %d}", vr.start, vr.end)
		olderCounts := make(map[string]map[string]map[string]map[string]float64)
		var searchAfter map[string]any = nil
		for {
			olderIndexTotalAgg, newSearchAfter, err := dbos.CompositePaginatedAggregate(ctx, client, 100, index.Index, query,
				olderIndexGroupBy, aggregations, searchAfter)
			if err != nil {
				return err
			}
			if len(olderIndexTotalAgg) == 0 {
				break
			}
			countStats(olderIndexTotalAgg, olderIndexGroupBy, olderCounts)
			searchAfter = newSearchAfter
		}
		searchAfter = nil
		newCounts := make(map[string]map[string]map[string]map[string]float64)
		newIndexGroupBy := []string{"tags.db_tenant_id.keyword", "tags.db_event_source_id.keyword", "name.raw", "namespace"}
		for {
			newIndexTotalAgg, newSearchAfter, err := dbos.CompositePaginatedAggregate(ctx, client, 100, newIndexName, query,
				newIndexGroupBy, aggregations, searchAfter)
			if err != nil {
				return err
			}
			if len(newIndexTotalAgg) == 0 {
				break
			}
			countStats(newIndexTotalAgg, newIndexGroupBy, newCounts)
			searchAfter = newSearchAfter
		}

		if !reflect.DeepEqual(olderCounts, newCounts) {
			logger.GetLogger().Info("new index data validation failed", zap.String("index", index.Index),
				zap.String("new_index", newIndexName), zap.Any("older_index_total_agg", olderCounts),
				zap.Any("new_index_total_agg", newCounts), zap.Int64("timeRange.Start", vr.start),
				zap.Int64("timeRange.End", vr.end))
			return errors.New("new index data validation failed")
		}
	}
	return nil
}

func countStats(indexTotalAgg []dbos.AggResponse, newIndexGroupBy []string, counts map[string]map[string]map[string]map[string]float64) {
	for _, agg := range indexTotalAgg {
		tenant := agg.Key[newIndexGroupBy[0]].(string)
		source := agg.Key[newIndexGroupBy[1]].(string)
		name := agg.Key[newIndexGroupBy[2]].(string)
		namespace := agg.Key[newIndexGroupBy[3]].(string)

		if counts[tenant] == nil {
			counts[tenant] = make(map[string]map[string]map[string]float64)
		}
		if counts[tenant][source] == nil {
			counts[tenant][source] = make(map[string]map[string]float64)
		}
		if counts[tenant][source][name] == nil {
			counts[tenant][source][name] = make(map[string]float64)
		}
		counts[tenant][source][name][namespace] = agg.Values["total_count"].(float64)
	}
}

func buildRolloverAggRequest(start int64, end int64, after *After, batchSize int, aggWindowDuration time.Duration) RolloverAggRequest {
	rolloverRequest := RolloverAggRequest{}
	rolloverRequest.Size = 0
	rolloverRequest.Query.Range.TagsDbTsWin.Gte = start
	rolloverRequest.Query.Range.TagsDbTsWin.Lt = end
	rolloverRequest.Aggs.CompositeBuckets.Composite.Size = batchSize
	requestTermsAggName := newRequestSourceTermsAgg("name.raw")
	requestSourceName := RequestSource{Name: &requestTermsAggName}
	requestTermsAggNamespace := newRequestSourceTermsAgg("namespace")
	requestSourceNamespace := RequestSource{Namespace: &requestTermsAggNamespace}
	requestTermsAggSourceId := newRequestSourceTermsAgg("tags.db_event_source_id.keyword")
	requestSourceSourceId := RequestSource{SourceId: &requestTermsAggSourceId}
	requestTermsAggDestinationId := newRequestSourceTermsAgg("tags.destination_id.keyword")
	requestSourceDestinationId := RequestSource{DestinationId: &requestTermsAggDestinationId}
	requestTermsAggRuleId := newRequestSourceTermsAgg("tags.rule_id.keyword")
	requestSourceRuleId := RequestSource{RuleId: &requestTermsAggRuleId}
	requestTermsAggFleetNodeId := newRequestSourceTermsAgg("tags.db_node_id.keyword")
	requestSourceFleetNodeId := RequestSource{FleetNodeId: &requestTermsAggFleetNodeId}
	timeHistogramBuckets := RequestSourceTimeHistogramBuckets{}
	timeHistogramBuckets.DateHistogram.Field = "tags.db_ts_win"
	timeHistogramBuckets.DateHistogram.FixedInterval = util.FormatDuration(aggWindowDuration)
	timeHistogramSource := RequestSource{
		TimeHistogramBuckets: &timeHistogramBuckets,
	}
	requestSources := []RequestSource{requestSourceName, requestSourceNamespace, requestSourceSourceId, requestSourceDestinationId, requestSourceRuleId, requestSourceFleetNodeId, timeHistogramSource}
	rolloverRequest.Aggs.CompositeBuckets.Composite.Sources = requestSources
	rolloverRequest.Aggs.CompositeBuckets.Composite.After = after

	topHits := TopHits{
		Source: "*",
		Size:   1,
	}
	rolloverRequest.Aggs.CompositeBuckets.Aggregations.AllFields.TopHits = topHits
	sum := Sum{
		Field: "counter.value",
	}
	rolloverRequest.Aggs.CompositeBuckets.Aggregations.TotalCount.Sum = sum
	return rolloverRequest
}

func insertIntoNewIndex(ctx context.Context, documents []EsSource, client *opensearch.Client, newIndexName string) error {
	if len(documents) == 0 {
		return nil
	}
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := fmt.Fprintf(buff, "{\"index\": {\"_id\": %s}}\n", strconv.Quote(doc.Id))
		if err != nil {
			return err
		}
		j, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		buff.Write(j)
		buff.Write([]byte("\n"))
	}
	request := opensearchapi.BulkRequest{
		Index: newIndexName,
		Body:  buff,
	}
	err := dbos.PerformBulkRequest(ctx, client, &request)
	if err != nil {
		return err
	}
	logger.GetLogger().Debug("indexed documents to rolled over index:"+newIndexName, zap.Int("count", len(documents)))
	return nil
}
