package os

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
	"io"
	"strconv"
	"strings"
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
	response, err := MakeSearchCall(ctx, index+"*", request, client)
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

func Search(ctx context.Context, client *opensearch.Client, index string, query string) ([]map[string]any, error) {
	request := SearchRequest{}
	request.Query.QueryString.Query = query
	response, err := MakeSearchCall(ctx, index+"*", request, client)
	if err != nil {
		return nil, err
	}
	if response.IsError() {
		msg := fmt.Sprintf("[%d] Status from OpenSearch body: %s", response.StatusCode, response.String())
		return nil, errors.New(msg)
	}
	bodyContent, _ := io.ReadAll(response.Body)
	searchResponse := SearchResponse{}
	err = json.Unmarshal(bodyContent, &searchResponse)
	if err != nil {
		return nil, err
	}
	if searchResponse.Error.Reason != "" {
		return nil, errors.New(searchResponse.Error.Reason)
	}
	data := make([]map[string]any, 0)
	for _, hit := range searchResponse.Hits.Hits {
		data = append(data, hit.Source)
	}
	return data, nil
}

func CompositePaginatedAggregate(ctx context.Context, cli *opensearch.Client, size int, indexName, query string, groupBy []string, aggregations []AggregationFunction, after map[string]any) ([]AggResponse, map[string]any, error) {
	req := CompositeAggRequest{}
	req.Size = 0
	req.Query.QueryString.Query = query
	req.Aggs.GroupBy.Composite.Sources = make([]map[string]SourceTerms, 0)
	for _, group := range groupBy {
		src := make(map[string]SourceTerms)
		src[group] = SourceTerms{Terms: Field{Field: group}}
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
	err := PerformBulkRequest(ctx, cli, &request)
	if err != nil {
		return err
	}
	logger.GetLogger().Debug("upserted documented with script", zap.Int("count", len(documents)), zap.String("index", indexName))
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

type Sort struct {
	Field string
	Order string
}

type SourceTerms struct {
	Terms struct {
		Field string `json:"field"`
	} `json:"terms"`
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
