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
	conf := dbos.GetConf()
	weeksOlderThan := utils.GetEnvInt("STATS_ROLLOVER_OLDER_THAN_WEEKS", 2)
	parallelism := utils.GetEnvInt("STATS_ROLLOVER_PARALLELISM", 4)
	limit := utils.GetEnvInt("STATS_ROLLOVER_INDEX_LIMIT", 10)
	specificIndex := utils.GetEnvOrDefault("STATS_ROLLOVER_SPECIFIC_INDEX", "")
	skipIndices := utils.GetEnvOrDefault("STATS_ROLLOVER_SKIP_INDICES", "")

	logger.GetLogger().Info("starting stats rollover", zap.Int("weeks_older_than", weeksOlderThan),
		zap.Int("parallelism", parallelism), zap.Int("limit", limit), zap.String("specific_index", specificIndex))

	var indexNames []string
	osClient, err := dbos.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("error while connecting to statistics store", zap.Error(err))
		return err
	}
	if specificIndex == "" {
		indexNames, err = dbos.CatIndices(ctx, osClient)
		if err != nil {
			logger.GetLogger().Error("error while fetching indices", zap.Error(err))
			return err
		}
	} else {
		indexNames = []string{specificIndex}
	}
	indicesToSkip := strings.Split(skipIndices, ",")
	indicesToRollover := filterStatsValidIndices(indexNames, weeksOlderThan, limit, indicesToSkip)
	logger.GetLogger().Info("indices to rollover", zap.Any("indices", indicesToRollover))

	errrCount := 0
	successCount := 0
	wg := sync.WaitGroup{}
	parallelismCntrl := make(chan struct{}, parallelism)
	for i, index := range indicesToRollover {
		wg.Add(1)
		parallelismCntrl <- struct{}{}
		indexCopy := index
		go func(j int, anIndex Index) {
			defer func() {
				wg.Done()
				<-parallelismCntrl
			}()
			err := rollover(ctx, anIndex, osClient)
			if err != nil {
				errrCount++
				logger.GetLogger().Error("error while rolling over index", zap.Error(err), zap.Any("index", anIndex), zap.Int("index_number", j))
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

type Index struct {
	Index  string
	Tenant string
	Year   int
	Week   int
}

type timeRange struct {
	start int64
	end   int64
}

func (in Index) aliasName() string {
	return fmt.Sprintf("db_statistics_alias_%s", in.Tenant)
}

func dailyTimeRanges(minEpoch, maxEpoch int64) []timeRange {
	var ranges []timeRange
	startTime := time.UnixMilli(minEpoch).UTC()
	endTime := time.UnixMilli(maxEpoch).UTC()
	startOfDay := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, time.UTC)
	for startOfDay.Before(endTime) {
		endOfDay := startOfDay.Add(24 * time.Hour)
		ranges = append(ranges, timeRange{start: startOfDay.UnixMilli(), end: endOfDay.UnixMilli()})
		startOfDay = endOfDay
	}
	ranges[len(ranges)-1].end = ranges[len(ranges)-1].end + 1
	return ranges
}

func (in Index) newIndexNameFor1HourRollover() string {
	return strings.ReplaceAll(in.Index, "db_statistics", "rolled_over_1h_db_statistics")
}

func (in Index) s3FileName(suffix string) string {
	return os.TempDir() + "/" + in.Index + "_" + suffix + ".txt.gz"
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false

}

func filterStatsValidIndices(indexNames []string, weeksOlderThan int, limit int, skip []string) []Index {
	thisWeek := getWeekOfYear()
	thisYear := time.Now().Year()
	thisYearWeekNumber := yearWeekNumber(thisYear, thisWeek)
	var indicesToRollover []Index
	for _, index := range indexNames {
		if strings.HasPrefix(index, "db_statistics_") {
			indexObj, yearWeek, ok := parseIndexName(index)
			if ok {
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

func parseIndexName(index string) (*Index, int, bool) {
	split := strings.Split(index, "_")
	if len(split) == 5 {
		year, err := strconv.Atoi(split[3])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[3]))
			return nil, 0, false
		}
		week, err := strconv.Atoi(split[4])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting week", zap.Error(err), zap.String("index", index), zap.String("week", split[4]))
			return nil, 0, false
		}
		tenantId := split[2]
		indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Week: week}
		if indexToRollover.Year != 1970 {
			return &indexToRollover, yearWeekNumber(year, week), true
		} else {
			logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
			return nil, 0, false
		}
	} else if len(split) == 6 {
		year, err := strconv.Atoi(split[4])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting year", zap.Error(err), zap.String("index", index), zap.String("year", split[5]))
			return nil, 0, false
		}
		week, err := strconv.Atoi(split[5])
		if err != nil {
			logger.GetLogger().Error("stats index name parsing: error while converting week", zap.Error(err), zap.String("index", index), zap.String("week", split[4]))
			return nil, 0, false
		}
		tenantId := split[2]
		indexToRollover := Index{Index: index, Tenant: tenantId, Year: year, Week: week}
		if indexToRollover.Year != 1970 {
			return &indexToRollover, yearWeekNumber(year, week), true
		} else {
			logger.GetLogger().Info("skipping index with year 1970", zap.String("index", index))
			return nil, 0, false
		}
	} else {
		logger.GetLogger().Error("stats index name parsing: unexpected index name format", zap.String("index", index))
		return nil, 0, false
	}
}

func rollover(ctx context.Context, index Index, client *opensearch.Client) error {
	logger.GetLoggerWithContext(ctx).Info("rolling over index", zap.String("index", index.Index))
	minVal, maxVal, err := findMinMaxTimestamp(ctx, index, client)
	if err != nil {
		return err
	}
	days := dailyTimeRanges(minVal, maxVal)
	newIndexValues := 0
	for _, day := range days {
		start := day.start
		end := day.end
		var after *After
		for {
			rolloverRequest := buildRolloverAggRequest(start, end, after)
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
				newDocId := compositeBucket.Key.newDocKey()
				counterSum := compositeBucket.TotalCount.Value
				sampleValues := compositeBucket.AllFields.Hits.Hits
				if len(sampleValues) == 0 {
					logger.GetLogger().Error("no AllFields found for agg key", zap.Any("key", compositeBucket.Key), zap.String("index", index.Index))
					return errors.New("no AllFields found for agg key")
				}
				sampleValue := sampleValues[0]
				newSource := sampleValue.Source
				newSource.Id = newDocId
				newSource.Counter.Value = counterSum
				newSource.Timestamp = timeHistogramBucket
				newSource.Tags["db_ts_win"] = timeHistogramBucket
				sources = append(sources, newSource)
				newIndexValues++
			}
			err = insertIntoNewIndex(ctx, index, sources, client)
			if err != nil {
				return err
			}
		}
	}
	err = validateNewData(ctx, index, client)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("rolled over index validated", zap.String("index", index.Index), zap.String("rolled_over_index", index.newIndexNameFor1HourRollover()))
	err = uploadOlderStatsToS3(ctx, index, client)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("rolled over original backed up", zap.String("index", index.Index), zap.String("rolled_over_index", index.newIndexNameFor1HourRollover()))
	err = updateAlias(index, client)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("rolled over alias updated", zap.String("index", index.Index), zap.String("rolled_over_index", index.newIndexNameFor1HourRollover()))
	err = dbos.DeleteIndex(ctx, client, index.Index)
	if err != nil {
		return err
	}
	logger.GetLogger().Info("deleted older index", zap.String("index", index.Index))
	logger.GetLogger().Info("rolled over index", zap.String("index", index.Index), zap.Int("new_index_values", newIndexValues))
	return nil
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

func updateAlias(index Index, client *opensearch.Client) error {
	alias := index.aliasName()
	return dbos.UpdateAliases(client, alias, index.Index, index.newIndexNameFor1HourRollover())
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
				err = upload(ctx, index, zipFile, zipWriter)
				if err != nil {
					return err
				}
				zipWriter = nil
				zipFile = nil
				fileHadPendingData = false
			}
		} else {
			break
		}
		i++
	}
	if zipFile != nil && fileHadPendingData {
		err := upload(ctx, index, zipFile, zipWriter)
		if err != nil {
			return err
		}
	}
	return nil
}

func upload(ctx context.Context, index Index, s3File *os.File, writer *gzip.Writer) error {
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

func validateNewData(ctx context.Context, index Index, client *opensearch.Client) error {
	olderIndexGroupBy := []string{"name.raw", "namespace"}
	aggregations := []dbos.AggregationFunction{
		dbos.AggregationFunction{
			Name:     "total_count",
			Field:    "counter.value",
			Function: "sum",
		},
	}
	olderIndexTotalAgg, _, err := dbos.CompositePaginatedAggregate(ctx, client, 500, index.Index, "*",
		olderIndexGroupBy, aggregations, nil)
	if err != nil {
		return err
	}
	newIndexGroupBy := []string{"name.raw", "namespace"}
	newIndexTotalAgg, _, err := dbos.CompositePaginatedAggregate(ctx, client, 500, index.newIndexNameFor1HourRollover(), "*",
		newIndexGroupBy, aggregations, nil)
	if err != nil {
		return err
	}
	olderCounts := make(map[string]map[string]float64)
	for _, agg := range olderIndexTotalAgg {
		name := agg.Key[olderIndexGroupBy[0]].(string)
		namespace := agg.Key[olderIndexGroupBy[1]].(string)
		if olderCounts[name] == nil {
			olderCounts[name] = make(map[string]float64)
		}
		olderCounts[name][namespace] = agg.Values["total_count"].(float64)
	}
	newCounts := make(map[string]map[string]float64)
	for _, agg := range newIndexTotalAgg {
		name := agg.Key[newIndexGroupBy[0]].(string)
		namespace := agg.Key[newIndexGroupBy[1]].(string)
		if newCounts[name] == nil {
			newCounts[name] = make(map[string]float64)
		}
		newCounts[name][namespace] = agg.Values["total_count"].(float64)
	}
	if !reflect.DeepEqual(olderCounts, newCounts) {
		logger.GetLogger().Info("new index data validation failed", zap.String("index", index.Index),
			zap.String("new_index", index.newIndexNameFor1HourRollover()), zap.Any("older_index_total_agg", olderIndexTotalAgg),
			zap.Any("new_index_total_agg", newIndexTotalAgg))
		return errors.New("new index data validation failed")
	}
	return nil
}

func buildRolloverAggRequest(start int64, end int64, after *After) RolloverAggRequest {
	rolloverRequest := RolloverAggRequest{}
	rolloverRequest.Size = 0
	rolloverRequest.Query.Range.TagsDbTsWin.Gte = start
	rolloverRequest.Query.Range.TagsDbTsWin.Lt = end
	rolloverRequest.Aggs.CompositeBuckets.Composite.Size = 1000
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
	timeHistogramBuckets.DateHistogram.FixedInterval = "1h"
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

func insertIntoNewIndex(ctx context.Context, index Index, documents []EsSource, client *opensearch.Client) error {
	if len(documents) == 0 {
		return nil
	}
	indexName := index.newIndexNameFor1HourRollover()
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
		Index: indexName,
		Body:  buff,
	}
	err := dbos.PerformBulkRequest(ctx, client, &request)
	if err != nil {
		return err
	}
	logger.GetLogger().Debug("indexed documents to rolled over index:"+indexName, zap.Int("count", len(documents)))
	return nil
}

func getWeekOfYear() int {
	_, week := time.Now().ISOWeek()
	return week
}

func yearWeekNumber(year, week int) int {
	return year*100 + week
}

type After struct {
	Name                 string `json:"name"`
	Namespace            string `json:"namespace"`
	SourceId             string `json:"source_id"`
	DestinationId        string `json:"destination_id"`
	RuleId               string `json:"rule_id"`
	FleetNodeId          string `json:"fleet_node_id"`
	TimeHistogramBuckets int64  `json:"time_histogram_buckets"`
}

type RequestSourceTermsAgg struct {
	Terms struct {
		Script ScripTerms `json:"script"`
	} `json:"terms"`
}

func newRequestSourceTermsAgg(field string) RequestSourceTermsAgg {
	script := fmt.Sprintf("if ((!doc.containsKey('%s')) || doc['%s'].size() == 0) { return 'N/A'; } else { return doc['%s'].value; }", field, field, field)
	terms := ScripTerms{
		Source: script,
		Lang:   "painless",
	}
	agg := RequestSourceTermsAgg{}
	agg.Terms.Script = terms
	return agg

}

type ScripTerms struct {
	Source string `json:"source"`
	Lang   string `json:"lang"`
}

type RequestSourceTimeHistogramBuckets struct {
	DateHistogram struct {
		Field         string `json:"field"`
		FixedInterval string `json:"fixed_interval"`
	} `json:"date_histogram"`
}

type RequestSource struct {
	Name                 *RequestSourceTermsAgg             `json:"name,omitempty"`
	Namespace            *RequestSourceTermsAgg             `json:"namespace,omitempty"`
	SourceId             *RequestSourceTermsAgg             `json:"source_id,omitempty"`
	DestinationId        *RequestSourceTermsAgg             `json:"destination_id,omitempty"`
	RuleId               *RequestSourceTermsAgg             `json:"rule_id,omitempty"`
	FleetNodeId          *RequestSourceTermsAgg             `json:"fleet_node_id,omitempty"`
	TimeHistogramBuckets *RequestSourceTimeHistogramBuckets `json:"time_histogram_buckets,omitempty"`
}

type TopHits struct {
	Source string `json:"_source"`
	Size   int    `json:"size"`
}

type Sum struct {
	Field string `json:"field"`
}

type RolloverAggRequest struct {
	Size  int `json:"size"`
	Query struct {
		Range struct {
			TagsDbTsWin struct {
				Gte int64 `json:"gte"`
				Lt  int64 `json:"lt"`
			} `json:"tags.db_ts_win"`
		} `json:"range"`
	} `json:"query"`
	Aggs struct {
		CompositeBuckets struct {
			Composite struct {
				After   *After          `json:"after,omitempty"`
				Size    int             `json:"size"`
				Sources []RequestSource `json:"sources"`
			} `json:"composite"`
			Aggregations struct {
				AllFields struct {
					TopHits TopHits `json:"top_hits"`
				} `json:"all_fields"`
				TotalCount struct {
					Sum Sum `json:"sum"`
				} `json:"total_count"`
			} `json:"aggregations"`
		} `json:"composite_buckets"`
	} `json:"aggs"`
}

type Error struct {
	RootCause []struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"root_cause"`
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type EsDoc struct {
	Index  string   `json:"_index"`
	Id     string   `json:"_id"`
	Score  float64  `json:"_score"`
	Source EsSource `json:"_source"`
}

type EsSource struct {
	AggKey    string `json:"agg_key"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	WindowKey string `json:"window_key"`
	Counter   struct {
		Value float64 `json:"value"`
	} `json:"counter"`
	Id        string         `json:"id"`
	Tags      map[string]any `json:"tags"`
	Timestamp any            `json:"timestamp"`
}

type Key struct {
	Name                 string `json:"name"`
	Namespace            string `json:"namespace"`
	SourceId             string `json:"source_id"`
	DestinationId        string `json:"destination_id"`
	RuleId               string `json:"rule_id"`
	FleetNodeId          string `json:"fleet_node_id"`
	TimeHistogramBuckets int64  `json:"time_histogram_buckets"`
}

func (k Key) newDocKey() string {
	val := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%d", k.Name, k.Namespace, k.SourceId, k.DestinationId, k.RuleId, k.FleetNodeId, k.TimeHistogramBuckets)
	return util.Hash(val)
}

type RolloverAggResponse struct {
	Error    *Error `json:"error"`
	Status   int    `json:"status"`
	Took     int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore interface{}   `json:"max_score"`
		Hits     []interface{} `json:"hits"`
	} `json:"hits"`
	Aggregations struct {
		CompositeBuckets struct {
			AfterKey *After `json:"after_key"`
			Buckets  []struct {
				Key       Key `json:"key"`
				DocCount  int `json:"doc_count"`
				AllFields struct {
					Hits struct {
						Total struct {
							Value    int    `json:"value"`
							Relation string `json:"relation"`
						} `json:"total"`
						MaxScore float64 `json:"max_score"`
						Hits     []EsDoc `json:"hits"`
					} `json:"hits"`
				} `json:"all_fields"`
				TotalCount struct {
					Value float64 `json:"value"`
				} `json:"total_count"`
			} `json:"buckets"`
		} `json:"composite_buckets"`
	} `json:"aggregations"`
}

type MinMaxRequest struct {
	Size int `json:"size"`
	Aggs struct {
		MinVal struct {
			Min struct {
				Field string `json:"field"`
			} `json:"min"`
		} `json:"min_val"`
		MaxVal struct {
			Max struct {
				Field string `json:"field"`
			} `json:"max"`
		} `json:"max_val"`
	} `json:"aggs"`
}

type MinMaxResponse struct {
	Error        *Error `json:"error"`
	Status       int    `json:"status"`
	Aggregations struct {
		MaxVal struct {
			Value float64 `json:"value"`
		} `json:"max_val"`
		MinVal struct {
			Value float64 `json:"value"`
		} `json:"min_val"`
	} `json:"aggregations"`
}
