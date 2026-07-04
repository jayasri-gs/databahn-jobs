package pramaan

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	osclient "github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"github.com/testcontainers/testcontainers-go"
	tcopensearch "github.com/testcontainers/testcontainers-go/modules/opensearch"
	"github.com/testcontainers/testcontainers-go/network"
)

/*
OpenSearchPramaan is a utility to start OpenSearch for module testing.
It can start OpenSearch with predefined indices and mappings to create and manage data for testing.
You can connect to the cluster and perform operations with it.
*/
type OpenSearchPramaan struct {
	t           TestLogger
	Container   *tcopensearch.OpenSearchContainer
	ExternalURL string
	NetworkURL  string
	client      *osclient.Client
	username    string
	password    string
	host        string
	port        string
}

const (
	opensearchInternalNetworkDns = "pramaanopensearch"
	opensearchPort               = "9200"
	opensearchDefaultUsername    = "admin"
	opensearchDefaultPassword    = "admin"
)

func NewOpenSearchPramaan(ctx context.Context, t TestLogger, dockerNetwork *testcontainers.DockerNetwork, extraTemplateDirs ...string) *OpenSearchPramaan {
	// Create OpenSearch container
	opensearchContainer, err := tcopensearch.Run(ctx, "opensearchproject/opensearch:2.11.0",
		network.WithNetwork([]string{opensearchInternalNetworkDns}, dockerNetwork),
	)
	if err != nil {
		t.Fatalf("Could not create opensearch container %v", err)
	}

	host, err := opensearchContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get host: %v", err)
	}

	port, err := opensearchContainer.MappedPort(ctx, opensearchPort)
	if err != nil {
		log.Fatalf("failed to get port: %v", err)
	}

	externalURL := fmt.Sprintf("http://%s:%d", host, port.Num())
	networkURL := fmt.Sprintf("http://%s:%s", opensearchInternalNetworkDns, opensearchPort)

	// Create OpenSearch client
	config := osclient.Config{
		Addresses: []string{externalURL},
		Username:  opensearchDefaultUsername,
		Password:  opensearchDefaultPassword,
	}

	client, err := osclient.NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create opensearch client: %v", err)
	}

	applyIndexTemplates(ctx, t, client, extraTemplateDirs...)

	return &OpenSearchPramaan{
		t:           t,
		Container:   opensearchContainer,
		ExternalURL: externalURL,
		NetworkURL:  networkURL,
		client:      client,
		username:    opensearchDefaultUsername,
		password:    opensearchDefaultPassword,
		host:        host,
		port:        port.Port(),
	}
}

type OpenSearchDocument struct {
	Index      string
	DocumentID string
	Raw        map[string]interface{}
}

func (d *OpenSearchDocument) Source() (map[string]interface{}, bool) {
	if d == nil || d.Raw == nil {
		return nil, false
	}
	rawSource, exists := d.Raw["_source"]
	if !exists || rawSource == nil {
		return nil, false
	}

	data, err := json.Marshal(rawSource)
	if err != nil {
		return nil, false
	}

	var source map[string]interface{}
	if err := json.Unmarshal(data, &source); err != nil {
		return nil, false
	}
	return source, true
}

/*
GetClient returns the OpenSearch client for direct operations.
*/
func (o *OpenSearchPramaan) GetClient() *osclient.Client {
	return o.client
}

/*
CreateIndex creates an index with the specified mapping.
*/
func (o *OpenSearchPramaan) CreateIndex(ctx context.Context, indexName string, mapping map[string]interface{}) error {
	var body io.Reader
	if mapping != nil {
		jsonData, err := json.Marshal(mapping)
		if err != nil {
			return fmt.Errorf("failed to marshal mapping: %v", err)
		}
		body = bytes.NewBuffer(jsonData)
	}

	req := opensearchapi.IndicesCreateRequest{
		Index: indexName,
		Body:  body,
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return fmt.Errorf("failed to create index: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create index, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

/*
DeleteIndex deletes an index if it exists.
*/
func (o *OpenSearchPramaan) DeleteIndex(ctx context.Context, indexName string) error {
	req := opensearchapi.IndicesDeleteRequest{
		Index: []string{indexName},
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return fmt.Errorf("failed to delete index: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete index, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

/*
IndexDocument indexes a document in the specified index.
*/
func (o *OpenSearchPramaan) IndexDocument(ctx context.Context, indexName, documentID string, document map[string]interface{}) error {
	jsonData, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %v", err)
	}

	req := opensearchapi.IndexRequest{
		Index:      indexName,
		DocumentID: documentID,
		Body:       bytes.NewBuffer(jsonData),
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return fmt.Errorf("failed to index document: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to index document, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

/*
SearchDocuments searches for documents in the specified index.
*/
func (o *OpenSearchPramaan) SearchDocuments(ctx context.Context, indexName string, query map[string]interface{}) (map[string]interface{}, error) {
	jsonData, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %v", err)
	}

	req := opensearchapi.SearchRequest{
		Index: []string{indexName},
		Body:  bytes.NewBuffer(jsonData),
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return nil, fmt.Errorf("failed to search documents: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to search documents, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return result, nil
}

/*
GetDocument retrieves a document by ID from the specified index.
*/
func (o *OpenSearchPramaan) GetDocument(ctx context.Context, indexName, documentID string) (map[string]interface{}, error) {
	req := opensearchapi.GetRequest{
		Index:      indexName,
		DocumentID: documentID,
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get document: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get document, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return result, nil
}

/*
UpdateDocument applies a partial update to a document in the specified index.
*/
func (o *OpenSearchPramaan) UpdateDocument(ctx context.Context, indexName, documentID string, fields map[string]interface{}) error {
	jsonData, err := json.Marshal(map[string]interface{}{
		"doc": fields,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal update: %v", err)
	}

	resp, err := o.client.Update(indexName, documentID, bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("failed to update document: %v", err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update document, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

/*
WaitForDocument waits until the document exists in the specified index.
*/
func (o *OpenSearchPramaan) WaitForDocument(
	ctx context.Context,
	indexName, documentID string,
	checkInterval, timeout time.Duration,
) *OpenSearchDocument {
	return o.WaitForCondition(ctx, indexName, documentID, OpenSearchDocumentExistsCondition{}, checkInterval, timeout)
}

/*
WaitForCondition waits for a specific condition to be met on an OpenSearch document.
It polls GetDocument at regular intervals until the condition is satisfied or the timeout is reached.
*/
func (o *OpenSearchPramaan) WaitForCondition(
	ctx context.Context,
	indexName, documentID string,
	condition Condition[*OpenSearchDocument],
	checkInterval, timeout time.Duration,
) *OpenSearchDocument {
	if timeout < checkInterval {
		o.t.Fatalf("Timeout should be greater than check interval")
	}

	timeOutTicker := time.NewTicker(timeout)
	checkTicker := time.NewTicker(checkInterval)
	defer timeOutTicker.Stop()
	defer checkTicker.Stop()

	for {
		select {
		case <-timeOutTicker.C:
			o.t.Fatalf("Timeout waiting for condition: %s", condition.String())
		case <-checkTicker.C:
			raw, err := o.GetDocument(ctx, indexName, documentID)
			if err != nil {
				continue
			}

			document := &OpenSearchDocument{
				Index:      indexName,
				DocumentID: documentID,
				Raw:        raw,
			}
			if condition.Verify(document) {
				return document
			}
		}
	}
}

/*
DeleteDocument deletes a document by ID from the specified index.
*/
func (o *OpenSearchPramaan) DeleteDocument(ctx context.Context, indexName, documentID string) error {
	req := opensearchapi.DeleteRequest{
		Index:      indexName,
		DocumentID: documentID,
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return fmt.Errorf("failed to delete document: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete document, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

/*
GetClusterHealth returns the cluster health information.
*/
func (o *OpenSearchPramaan) GetClusterHealth(ctx context.Context) (map[string]interface{}, error) {
	req := opensearchapi.ClusterHealthRequest{}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get cluster health, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return result, nil
}

/*
CreateIndexTemplate creates an index template that will be applied to indices matching the specified pattern.
The template can include mappings, settings, and other index configurations.
*/
func (o *OpenSearchPramaan) CreateIndexTemplate(ctx context.Context, templateName string, indexPatterns []string, template map[string]interface{}) error {
	// Add index_patterns to the template
	template["index_patterns"] = indexPatterns

	jsonData, err := json.Marshal(template)
	if err != nil {
		return fmt.Errorf("failed to marshal template: %v", err)
	}

	req := opensearchapi.IndicesPutTemplateRequest{
		Name: templateName,
		Body: bytes.NewBuffer(jsonData),
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return fmt.Errorf("failed to create index template: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create index template, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

/*
DeleteIndexTemplate deletes an index template.
*/
func (o *OpenSearchPramaan) DeleteIndexTemplate(ctx context.Context, templateName string) error {
	req := opensearchapi.IndicesDeleteTemplateRequest{
		Name: templateName,
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return fmt.Errorf("failed to delete index template: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete index template, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

/*
GetIndexTemplate retrieves an index template by name.
*/
func (o *OpenSearchPramaan) GetIndexTemplate(ctx context.Context, templateName string) (map[string]interface{}, error) {
	req := opensearchapi.IndicesGetTemplateRequest{
		Name: []string{templateName},
	}

	resp, err := req.Do(ctx, o.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get index template: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get index template, status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return result, nil
}

/*
GetConnectionInfo returns the connection information for the OpenSearch instance.
*/
func (o *OpenSearchPramaan) GetConnectionInfo() map[string]string {
	return map[string]string{
		"host":        o.host,
		"port":        o.port,
		"username":    o.username,
		"password":    o.password,
		"url":         o.ExternalURL,
		"network_url": o.NetworkURL,
	}
}

/*
GetUsername returns the OpenSearch username.
*/
func (o *OpenSearchPramaan) GetUsername() string {
	return o.username
}

/*
GetPassword returns the OpenSearch password.
*/
func (o *OpenSearchPramaan) GetPassword() string {
	return o.password
}

/*
GetHost returns the OpenSearch host.
*/
func (o *OpenSearchPramaan) GetHost() string {
	return o.host
}

/*
GetPort returns the OpenSearch port.
*/
func (o *OpenSearchPramaan) GetPort() string {
	return o.port
}

/*
GetExternalURL returns the external URL for accessing OpenSearch.
*/
func (o *OpenSearchPramaan) GetExternalURL() string {
	return o.ExternalURL
}

/*
GetNetworkURL returns the internal network URL for accessing OpenSearch.
*/
func (o *OpenSearchPramaan) GetNetworkURL() string {
	return o.NetworkURL
}
