package os

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
)

func CatIndices(ctx context.Context, client *opensearch.Client) ([]string, error) {
	response, err := client.Cat.Indices()
	if err != nil {
		return []string{}, err
	}

	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return []string{}, errors.New(msg)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		msg := fmt.Sprintf("[%d] Error reading response body: %s", response.StatusCode, err.Error())
		return []string{}, errors.New(msg)
	}

	output := string(body)
	rows := strings.Split(output, "\n")
	var indexNames []string
	for _, row := range rows {
		indexDetails := strings.Split(row, " ")
		if len(indexDetails) > 2 {
			indexName := indexDetails[2]
			indexNames = append(indexNames, indexName)
		}
	}
	return indexNames, nil
}

func CatIndicesWithSize(ctx context.Context, client *opensearch.Client) (map[string]int64, error) {
	response, err := client.Cat.Indices(
		client.Cat.Indices.WithH("index", "pri.store.size"),
		client.Cat.Indices.WithBytes("b"),
	)
	if err != nil {
		return nil, err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return nil, errors.New(msg)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		msg := fmt.Sprintf("[%d] Error reading response body: %s", response.StatusCode, err.Error())
		return nil, errors.New(msg)
	}
	result := make(map[string]int64)
	for _, row := range strings.Split(string(body), "\n") {
		fields := strings.Fields(row)
		if len(fields) >= 2 {
			size, _ := strconv.ParseInt(fields[1], 10, 64)
			result[fields[0]] = size
		}
	}
	return result, nil
}

func UpdateAliases(client *opensearch.Client, alias, from, to string) error {
	aliasActions := `
	{
	  "actions": [
	    { "remove": { "index": "%s", "alias": "%s" } },
	    { "add":    { "index": "%s", "alias": "%s" } }
	  ]
	}`
	aliasActionsRequest := []byte(fmt.Sprintf(aliasActions, from, alias, to, alias))
	response, err := client.Indices.UpdateAliases(bytes.NewReader(aliasActionsRequest))
	if err != nil {
		return err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return errors.New(msg)
	}
	return nil
}

func UpdateMultipleAliases(client *opensearch.Client, alias string, from []string, to string) error {
	var removeActions []string
	for _, f := range from {
		removeActions = append(removeActions, fmt.Sprintf(`{ "remove": { "index": "%s", "alias": "%s" } }`, f, alias))
	}
	removeActionJoined := strings.Join(removeActions, ",\n")
	aliasActions := `
	{
	  "actions": [
	    %s,
	    { "add":    { "index": "%s", "alias": "%s" } }
	  ]
	}`
	aliasActionsRequest := []byte(fmt.Sprintf(aliasActions, removeActionJoined, to, alias))
	response, err := client.Indices.UpdateAliases(bytes.NewReader(aliasActionsRequest))
	if err != nil {
		return err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return errors.New(msg)
	}
	return nil
}

func RefreshIndex(ctx context.Context, client *opensearch.Client, indexName string) error {
	refreshIndex := opensearchapi.IndicesRefreshRequest{
		Index: []string{indexName},
	}
	response, err := refreshIndex.Do(ctx, client)
	if err != nil {
		return err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return errors.New(msg)
	}
	return nil
}

func DeleteIndex(ctx context.Context, client *opensearch.Client, indexName string) error {
	deleteIndex := opensearchapi.IndicesDeleteRequest{
		Index: []string{indexName},
	}
	response, err := deleteIndex.Do(ctx, client)
	if err != nil {
		return err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return errors.New(msg)
	}
	return nil
}

func MakeSearchCall(ctx context.Context, index string, searchBody interface{}, client *opensearch.Client) (*opensearchapi.Response, error) {
	bodyContent, err := json.Marshal(searchBody)
	if err != nil {
		return nil, err
	}
	logger.GetLogger().Debug("request", zap.String("body", string(bodyContent)), zap.String("index", index))
	bodyReader := bytes.NewReader(bodyContent)
	searchRequest := opensearchapi.SearchRequest{
		Index: []string{index},
		Body:  bodyReader,
	}
	searchResponse, err := searchRequest.Do(ctx, client)
	return searchResponse, err
}

func SearchPaginated(ctx context.Context, client *opensearch.Client, index string, query string, size int, after []any, sort []Sort) ([]map[string]any, []any, error) {
	sortBy := make([]map[string]string, 0)
	for _, s := range sort {
		sortBy = append(sortBy, map[string]string{s.Field: s.Order})
	}
	request := SearchRequestPaginated{}
	request.Size = size
	request.Query.QueryString.Query = query
	request.SearchAfter = after
	request.Sort = sortBy
	response, err := MakeSearchCall(ctx, index, request, client)
	if err != nil {
		return nil, nil, err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return nil, nil, errors.New(msg)
	}

	bodyContent, _ := io.ReadAll(response.Body)
	searchResponse := SearchResponse{}
	err = json.Unmarshal(bodyContent, &searchResponse)
	if err != nil {
		return nil, nil, err
	}

	if searchResponse.Error.Reason != "" {
		return nil, nil, errors.New(searchResponse.Error.Reason)
	}

	data := make([]map[string]any, 0)
	searchAfter := make([]any, 0)
	for _, hit := range searchResponse.Hits.Hits {
		data = append(data, hit.Source)
		searchAfter = hit.Sort
	}

	return data, searchAfter, nil
}

func Search(ctx context.Context, client *opensearch.Client, index string, query string) ([]map[string]any, int, error) {
	request := SearchRequest{}
	request.Query.QueryString.Query = query
	response, err := MakeSearchCall(ctx, index+"*", request, client)
	if err != nil {
		return nil, 0, err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return nil, 0, errors.New(msg)
	}
	bodyContent, _ := io.ReadAll(response.Body)
	searchResponse := SearchResponse{}
	err = json.Unmarshal(bodyContent, &searchResponse)
	if err != nil {
		return nil, 0, err
	}
	if searchResponse.Error.Reason != "" {
		return nil, 0, errors.New(searchResponse.Error.Reason)
	}
	data := make([]map[string]any, 0)
	for _, hit := range searchResponse.Hits.Hits {
		data = append(data, hit.Source)
	}
	return data, searchResponse.Hits.Total.Value, nil
}

func CompositePaginatedAggregate(ctx context.Context, cli *opensearch.Client, size int, indexName, query string, groupBy []string, aggregations []AggregationFunction, after map[string]any) ([]AggResponse, map[string]any, error) {
	return compositePaginatedAggregate(ctx, cli, size, indexName, query, groupBy, nil, aggregations, after)
}

// CompositePaginatedAggregateWithNAMissing uses a painless script for listed groupBy fields so
// documents missing that field bucket as "N/A", matching stats rollover aggregation behavior.
func CompositePaginatedAggregateWithNAMissing(ctx context.Context, cli *opensearch.Client, size int, indexName, query string, groupBy []string, missingAsNA []string, aggregations []AggregationFunction, after map[string]any) ([]AggResponse, map[string]any, error) {
	return compositePaginatedAggregate(ctx, cli, size, indexName, query, groupBy, missingAsNA, aggregations, after)
}

func compositePaginatedAggregate(ctx context.Context, cli *opensearch.Client, size int, indexName, query string, groupBy []string, missingAsNA []string, aggregations []AggregationFunction, after map[string]any) ([]AggResponse, map[string]any, error) {
	missingAsNASet := make(map[string]bool, len(missingAsNA))
	for _, field := range missingAsNA {
		missingAsNASet[field] = true
	}

	req := CompositeAggRequest{}
	req.Size = 0
	req.Query.QueryString.Query = query
	req.Aggs.GroupBy.Composite.Sources = make([]map[string]SourceTerms, 0)
	for _, group := range groupBy {
		src := make(map[string]SourceTerms)
		if missingAsNASet[group] {
			src[group] = sourceTermsMissingAsNA(group)
		} else {
			terms := SourceTerms{}
			terms.Terms.Field = group
			src[group] = terms
		}
		req.Aggs.GroupBy.Composite.Sources = append(req.Aggs.GroupBy.Composite.Sources, src)
	}
	req.Aggs.GroupBy.Composite.After = after
	req.Aggs.GroupBy.Composite.Size = size
	req.Aggs.GroupBy.Aggs = make(map[string]map[string]Field, len(aggregations))
	for _, agg := range aggregations {
		name := agg.Name
		req.Aggs.GroupBy.Aggs[name] = make(map[string]Field)
		req.Aggs.GroupBy.Aggs[name][agg.Function] = Field{Field: agg.Field}
	}
	response, err := MakeSearchCall(ctx, indexName+"*", req, cli)
	if err != nil {
		logger.GetLogger().Error("failed to make composite paginated agg call", zap.Error(err))
		return nil, nil, err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return nil, nil, errors.New(msg)
	}
	bodyContent, _ := io.ReadAll(response.Body)
	aggResponse := &CompositeAggregationResponse{}
	err = json.Unmarshal(bodyContent, aggResponse)
	if err != nil {
		return nil, nil, err
	}
	if aggResponse.Error.Reason != "" {
		return nil, nil, errors.New(aggResponse.Error.Reason)
	}
	var responses []AggResponse
	aggFunNames := make(map[string]bool)
	for _, agg := range aggregations {
		aggFunNames[agg.Name] = true
	}
	for _, bucket := range aggResponse.Aggregations.GroupBy.Buckets {
		resp := AggResponse{}
		resp.Key = make(map[string]any)
		resp.Values = make(map[string]any)
		for name, val := range bucket {
			if name == "key" {
				valueObj := val.(map[string]any)
				resp.Key = valueObj
			} else if aggFunNames[name] {
				valObj := val.(map[string]any)
				resp.Values[name] = valObj["value"]
			}
		}
		responses = append(responses, resp)
	}
	return responses, aggResponse.Aggregations.GroupBy.AfterKey, nil
}

func BulkUpsert[T any](ctx context.Context, cli *opensearch.Client, indexName string, documents []T, idExtractor func(T) string) error {
	if len(documents) == 0 {
		return nil
	}
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		id := idExtractor(doc)
		_, err := fmt.Fprintf(buff, "{\"index\": {\"_id\": %s}}\n", strconv.Quote(id))
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
	err := PerformBulkRequest(ctx, cli, &request)
	if err != nil {
		return err
	}
	logger.GetLogger().Debug("bulk upserted documents", zap.Int("count", len(documents)), zap.String("index", indexName))
	return nil
}

func BulkUpsertWithScript[T any](ctx context.Context, cli *opensearch.Client, indexName string, script string, documents []T, idExtractor func(T) string) error {
	if len(documents) == 0 {
		return nil
	}
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		id := idExtractor(doc)
		_, err := fmt.Fprintf(buff, "{\"update\": {\"_id\": %s}}\n", strconv.Quote(id))
		if err != nil {
			return err
		}
		j, err := getUpdateRequestBody(doc, script)
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
	successCount, missingDocs, err := PerformBulkRequestWithRetry(ctx, cli, &request)
	if err != nil {
		return err
	}

	if len(missingDocs) > 0 {
		logger.GetLogger().Warn("some documents were not found during bulk update",
			zap.Int("missing_count", len(missingDocs)),
			zap.Int("success_count", successCount),
			zap.String("index", indexName),
			zap.Strings("missing_ids", missingDocs))
	}

	logger.GetLogger().Debug("upserted documented with script", zap.Int("count", successCount), zap.String("index", indexName))
	return nil
}

func getUpdateRequestBody[T any](doc T, script string) ([]byte, error) {
	b := strings.ReplaceAll(script, "\n", " ")
	return utils.ParseTemplate([]byte(b), doc)
}

func PerformBulkRequest(ctx context.Context, cli *opensearch.Client, request *opensearchapi.BulkRequest) error {
	resp, err := request.Do(ctx, cli)
	if err != nil {
		return err
	} else {
		if resp.IsError() {
			return errors.New(resp.String())
		} else {
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			bodyJ := BaseEsResponse{}
			err = json.Unmarshal(bodyBytes, &bodyJ)
			if err != nil {
				return err
			}
			if bodyJ.Errors {
				return errors.New(string(bodyBytes))
			}
		}
	}
	return nil
}

// PerformBulkRequestWithRetry performs a bulk request and handles partial failures gracefully.
// It returns the count of successful operations, IDs of missing documents, and any error.
func PerformBulkRequestWithRetry(ctx context.Context, cli *opensearch.Client, request *opensearchapi.BulkRequest) (int, []string, error) {
	resp, err := request.Do(ctx, cli)
	if err != nil {
		return 0, nil, err
	}

	if resp.IsError() {
		return 0, nil, errors.New(resp.String())
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}

	bulkResp := BulkResponse{}
	err = json.Unmarshal(bodyBytes, &bulkResp)
	if err != nil {
		return 0, nil, err
	}

	// If no errors, all succeeded
	if !bulkResp.Errors {
		return len(bulkResp.Items), nil, nil
	}

	// Parse the response to identify successful vs failed items
	successCount := 0
	var missingDocs []string
	var otherErrors []string

	for _, item := range bulkResp.Items {
		// Each item is a map with one key (the operation type: index, update, delete, etc.)
		for opType, result := range item {
			if result.Status >= 200 && result.Status < 300 {
				successCount++
			} else if result.Error.Type == "document_missing_exception" {
				missingDocs = append(missingDocs, result.Id)
				logger.GetLogger().Debug("document not found during bulk update",
					zap.String("id", result.Id),
					zap.String("operation", opType),
					zap.Int("status", result.Status))
			} else {
				// Other types of errors
				otherErrors = append(otherErrors, fmt.Sprintf("ID: %s, Status: %d, Error: %s - %s",
					result.Id, result.Status, result.Error.Type, result.Error.Reason))
			}
		}
	}

	// If there are errors other than missing documents, return an error
	if len(otherErrors) > 0 {
		errMsg := fmt.Sprintf("bulk operation had %d errors (non-missing): %v", len(otherErrors), strings.Join(otherErrors, "; "))
		return successCount, missingDocs, errors.New(errMsg)
	}

	return successCount, missingDocs, nil
}

type Sort struct {
	Field string
	Order string
}

type SourceTerms struct {
	Terms struct {
		Field  string       `json:"field,omitempty"`
		Script *ScriptTerms `json:"script,omitempty"`
	} `json:"terms"`
}

type ScriptTerms struct {
	Source string `json:"source"`
	Lang   string `json:"lang"`
}

func sourceTermsMissingAsNA(field string) SourceTerms {
	script := fmt.Sprintf("if ((!doc.containsKey('%s')) || doc['%s'].size() == 0) { return 'N/A'; } else { return doc['%s'].value; }", field, field, field)
	terms := SourceTerms{}
	terms.Terms.Script = &ScriptTerms{Source: script, Lang: "painless"}
	return terms
}

type Field struct {
	Field string `json:"field"`
}

type CompositeAggRequest struct {
	Size  int `json:"size"`
	Query struct {
		QueryString struct {
			Query string `json:"query"`
		} `json:"query_string"`
	} `json:"query"`
	Aggs struct {
		GroupBy struct {
			Composite struct {
				Size    int                      `json:"size"`
				After   map[string]any           `json:"after,omitempty"`
				Sources []map[string]SourceTerms `json:"sources"`
			} `json:"composite"`
			Aggs map[string]map[string]Field `json:"aggs"`
		} `json:"group_by"`
	} `json:"aggs"`
}

type CompositeAggregationBucketValue struct {
	Value any `json:"value"`
}

type CompositeAggregationBucket struct {
	Key      map[string]any `json:"key"`
	DocCount int64          `json:"doc_count"`
}

type AggResponse struct {
	Key    map[string]any `json:"key"`
	Values map[string]any `json:"values"`
}

type CompositeAggregationResponse struct {
	ErrorResponse
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Hits     struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore interface{}   `json:"max_score"`
		Hits     []interface{} `json:"hits"`
	} `json:"hits"`
	Aggregations struct {
		GroupBy struct {
			AfterKey map[string]any   `json:"after_key"`
			Buckets  []map[string]any `json:"buckets"`
		} `json:"group_by"`
	} `json:"aggregations"`
}

type AggregationFunction struct {
	Function string
	Field    string
	Name     string
}

type BaseEsResponse struct {
	Errors bool `json:"errors"`
}

type BulkResponse struct {
	Took   int                           `json:"took"`
	Errors bool                          `json:"errors"`
	Items  []map[string]BulkResponseItem `json:"items"`
}

type BulkResponseItem struct {
	Index       string `json:"_index"`
	Id          string `json:"_id"`
	Version     int    `json:"_version,omitempty"`
	Result      string `json:"result,omitempty"`
	Status      int    `json:"status"`
	SeqNo       int64  `json:"_seq_no,omitempty"`
	PrimaryTerm int    `json:"_primary_term,omitempty"`
	Shards      struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_shards,omitempty"`
	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
		Index  string `json:"index,omitempty"`
		Shard  string `json:"shard,omitempty"`
	} `json:"error,omitempty"`
}

type SearchRequestPaginated struct {
	Size  int `json:"size"`
	Query struct {
		QueryString struct {
			Query string `json:"query"`
		} `json:"query_string"`
	} `json:"query"`
	SearchAfter []any               `json:"search_after,omitempty"`
	Sort        []map[string]string `json:"sort"`
}
type SearchRequest struct {
	Query struct {
		QueryString struct {
			Query string `json:"query"`
		} `json:"query_string"`
	} `json:"query"`
}
type SearchResponse struct {
	ErrorResponse
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		MaxScore interface{} `json:"max_score"`
		Hits     []struct {
			Id     string         `json:"_id"`
			Score  interface{}    `json:"_score"`
			Source map[string]any `json:"_source"`
			Sort   []any          `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
}

type ErrorResponse struct {
	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

// HistogramBucket represents a single bucket in the histogram aggregation
type HistogramBucket struct {
	Time  int64 `json:"time"`  // Epoch milliseconds
	Value int64 `json:"value"` // Aggregated value
}

// HistogramResponse contains the histogram buckets
type HistogramResponse struct {
	Buckets []HistogramBucket `json:"buckets"`
}

// HistogramAggRequest represents the request structure for histogram aggregation
type HistogramAggRequest struct {
	Size  int `json:"size"`
	Query struct {
		QueryString struct {
			Query string `json:"query"`
		} `json:"query_string"`
	} `json:"query"`
	Aggs struct {
		Histogram struct {
			DateHistogram struct {
				Field         string `json:"field"`
				FixedInterval string `json:"fixed_interval"`
				MinDocCount   int    `json:"min_doc_count"`
			} `json:"date_histogram"`
			Aggs map[string]map[string]Field `json:"aggs,omitempty"`
		} `json:"histogram"`
	} `json:"aggs"`
}

// HistogramAggregationResponse represents the response from histogram aggregation
type HistogramAggregationResponse struct {
	ErrorResponse
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Hits     struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
	} `json:"hits"`
	Aggregations struct {
		Histogram struct {
			Buckets []map[string]any `json:"buckets"`
		} `json:"histogram"`
	} `json:"aggregations"`
}

// GetHistogram performs a date histogram aggregation on the given index
// Returns histogram buckets with time (epoch millis) and aggregated value
func GetHistogram(
	ctx context.Context,
	client *opensearch.Client,
	index string,
	query string,
	timeField string,
	agg AggregationFunction,
	interval string,
) (*HistogramResponse, error) {

	// Build the request
	req := HistogramAggRequest{}
	req.Size = 0 // We only want aggregation results, not documents
	req.Query.QueryString.Query = query
	req.Aggs.Histogram.DateHistogram.Field = timeField
	req.Aggs.Histogram.DateHistogram.FixedInterval = interval
	req.Aggs.Histogram.DateHistogram.MinDocCount = 0 // Include empty buckets

	// Add the aggregation function (e.g., sum, avg, max, min)
	if agg.Name != "" {
		req.Aggs.Histogram.Aggs = make(map[string]map[string]Field)
		req.Aggs.Histogram.Aggs[agg.Name] = make(map[string]Field)
		req.Aggs.Histogram.Aggs[agg.Name][agg.Function] = Field{Field: agg.Field}
	}

	// Make the search call
	response, err := MakeSearchCall(ctx, index, req, client)
	if err != nil {
		logger.GetLogger().Error("failed to make histogram aggregation call", zap.Error(err))
		return nil, err
	}

	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return nil, errors.New(msg)
	}

	// Parse the response
	bodyContent, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	aggResponse := &HistogramAggregationResponse{}
	err = json.Unmarshal(bodyContent, aggResponse)
	if err != nil {
		return nil, err
	}

	if aggResponse.Error.Reason != "" {
		return nil, errors.New(aggResponse.Error.Reason)
	}

	// Extract buckets
	histogramResp := &HistogramResponse{
		Buckets: make([]HistogramBucket, 0),
	}

	for _, bucket := range aggResponse.Aggregations.Histogram.Buckets {
		// Extract time (key_as_string or key)
		timeMs := int64(0)
		if keyVal, ok := bucket["key"].(float64); ok {
			timeMs = int64(keyVal)
		}

		// Extract aggregated value
		value := int64(0)
		if agg.Name != "" {
			if aggVal, ok := bucket[agg.Name].(map[string]any); ok {
				if val, ok := aggVal["value"].(float64); ok {
					value = int64(val)
				}
			}
		} else {
			// If no aggregation specified, use doc_count
			if docCount, ok := bucket["doc_count"].(float64); ok {
				value = int64(docCount)
			}
		}

		histogramBucket := HistogramBucket{
			Time:  timeMs,
			Value: value,
		}
		histogramResp.Buckets = append(histogramResp.Buckets, histogramBucket)
	}

	return histogramResp, nil
}
