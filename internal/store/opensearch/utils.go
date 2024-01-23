package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"github.com/pkg/errors"
	"io"
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
	bodyContent, _ := json.Marshal(searchBody)
	bodyReader := bytes.NewReader(bodyContent)
	searchRequest := opensearchapi.SearchRequest{
		Index: []string{index},
		Body:  bodyReader,
	}
	searchResponse, err := searchRequest.Do(ctx, client)
	return searchResponse, err
}
