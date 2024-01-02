package store

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/databahn-ai/common-utils/configuration"
	"io"
	"net/http"
	"strings"

	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
)

type OpenSearchCreds struct {
	Username string
	Password string
}

func NewOpenSearchClient(ctx context.Context, conf configuration.ConfigReader) (*opensearch.Client, error) {
	secretName := conf.GetString(configuration.OpenSearchSecretName)
	os, err := configuration.ReadOpenSearchSecrets(context.Background(), secretName)
	if err != nil {
		return nil, err
	}
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
			},
		},
		Addresses: []string{os.Url},
		Username:  os.Username,
		Password:  os.Password,
	})
	if err != nil {
		logger.GetLoggerWithContext(ctx).Error("Failed to create Open search client", zap.Error(err))
	}
	return client, err
}

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
