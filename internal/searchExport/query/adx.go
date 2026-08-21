package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/unload"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	logging "github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	// adxStagingSASExpiry must comfortably outlive a long-running .export.
	adxStagingSASExpiry = 24 * time.Hour
	adxHTTPTimeout      = 5 * time.Minute
	adxPollInterval     = 5 * time.Second
)

// ADXConfig holds everything needed to run an .export against an Azure Data Explorer
// cluster and read the staged output back out of blob storage.
type ADXConfig struct {
	ClusterURI   string
	Database     string
	TenantID     string
	ClientID     string
	ClientSecret string

	// NamePrefix namespaces one report's exported blobs. Stable across retries.
	NamePrefix string

	// StagingBlob is the container .export writes into — the export destination's
	// own container, reused as staging.
	StagingBlob *destination.AzureBlobConfig

	QueryTimeout time.Duration
}

// ADXExecutor runs KQL and control commands over the Kusto v1 REST API.
// It implements UnloadExecutor: .export async yields a durable operation id, so a
// stale PROCESSING report reattaches instead of re-running the query.
type ADXExecutor struct {
	cfg        ADXConfig
	cred       *azidentity.ClientSecretCredential
	httpClient *http.Client
	token      azcore.AccessToken
	log        *zap.Logger
}

func NewADXExecutor(cfg ADXConfig) *ADXExecutor {
	if cfg.QueryTimeout == 0 {
		cfg.QueryTimeout = 30 * time.Minute
	}
	cfg.ClusterURI = strings.TrimSuffix(strings.TrimSpace(cfg.ClusterURI), "/")
	return &ADXExecutor{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: adxHTTPTimeout},
		log:        logging.GetLogger(),
	}
}

func (e *ADXExecutor) SetLogger(log *zap.Logger) {
	if log != nil {
		e.log = log
	}
}

func (e *ADXExecutor) Engine() string { return EngineADX }

// Connect builds the AAD credential and acquires a first token, which doubles as a
// credential check before any export work starts.
func (e *ADXExecutor) Connect(ctx context.Context) error {
	if e.cfg.ClusterURI == "" {
		return fmt.Errorf("adx_cluster_uri is required in connector configuration")
	}
	cred, err := azidentity.NewClientSecretCredential(e.cfg.TenantID, e.cfg.ClientID, e.cfg.ClientSecret, nil)
	if err != nil {
		return fmt.Errorf("create ADX service principal credential: %w", err)
	}
	e.cred = cred
	if _, err := e.accessToken(ctx); err != nil {
		return err
	}
	e.log.Info("Connected to Azure Data Explorer",
		zap.String("clusterUri", e.cfg.ClusterURI),
		zap.String("database", e.cfg.Database))
	return nil
}

func (e *ADXExecutor) Close() error { return nil }

// GetAWSConfig satisfies BaseExecutor; ADX has no AWS session.
func (e *ADXExecutor) GetAWSConfig() interface{} { return nil }

// GetOutputLocation returns the blob name prefix .export writes under.
func (e *ADXExecutor) GetOutputLocation() string { return e.cfg.NamePrefix }

// NewStagingReader reads the exported blobs back out of the staging container.
func (e *ADXExecutor) NewStagingReader(tempDir string) (unload.StagingReader, error) {
	if e.cfg.StagingBlob == nil {
		return nil, fmt.Errorf("ADX staging blob config is required")
	}
	client, err := destination.NewAzureBlobClient(e.cfg.StagingBlob)
	if err != nil {
		return nil, fmt.Errorf("ADX staging blob client: %w", err)
	}
	return unload.NewBlobReader(client, e.cfg.StagingBlob.Container, tempDir)
}

// ExecuteUnloadAsync starts an async .export and returns its operation id.
// outputPath is ignored: Kusto namespaces exported blobs by namePrefix, not by path.
func (e *ADXExecutor) ExecuteUnloadAsync(ctx context.Context, query, database, outputPath string, opts UnloadOptions) (string, error) {
	if e.cfg.StagingBlob == nil {
		return "", fmt.Errorf("ADX staging blob config is required")
	}
	staging, err := destination.GenerateContainerWriteSAS(ctx, e.cfg.StagingBlob, adxStagingSASExpiry)
	if err != nil {
		return "", fmt.Errorf("ADX staging SAS: %w", err)
	}

	format := ADXExportFormat(opts)
	command := BuildADXExportCommand(query, staging.KustoConnectionString(), format, e.cfg.NamePrefix)

	e.log.Info("Starting async ADX export",
		zap.String("database", e.resolveDatabase(database)),
		zap.String("exportFormat", format),
		zap.String("namePrefix", e.cfg.NamePrefix),
		zap.String("stagingContainer", staging.ContainerURL()))

	body, err := e.mgmt(ctx, database, command)
	if err != nil {
		return "", fmt.Errorf("start ADX export: %w", err)
	}
	operationID, err := ParseADXOperationID(body)
	if err != nil {
		return "", fmt.Errorf("start ADX export: %w", err)
	}
	e.log.Info("Started async ADX export", zap.String("adxOperationId", operationID))
	return operationID, nil
}

// CheckQueryStatus maps the Kusto operation state onto a pipeline query state.
func (e *ADXExecutor) CheckQueryStatus(ctx context.Context, executionID string) (string, error) {
	body, err := e.mgmt(ctx, "", BuildADXShowOperationCommand(executionID))
	if err != nil {
		return "", fmt.Errorf("get ADX operation status: %w", err)
	}
	state, _, err := ParseADXOperationStatus(body)
	if err != nil {
		return "", fmt.Errorf("get ADX operation status: %w", err)
	}
	return MapADXOperationState(state), nil
}

// WaitForExecution polls an export operation to completion, cancelling it on timeout.
func (e *ADXExecutor) WaitForExecution(ctx context.Context, executionID string) error {
	deadline := time.Now().Add(e.cfg.QueryTimeout)
	started := time.Now()
	lastLog := started

	for {
		if time.Now().After(deadline) {
			cancelCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := e.CancelQueryExecution(cancelCtx, executionID); err != nil {
				e.log.Warn("Failed to cancel timed-out ADX export", zap.Error(err))
			}
			cancel()
			e.log.Warn("ADX export timed out — operation cancelled",
				zap.String("adxOperationId", executionID),
				zap.Duration("timeout", e.cfg.QueryTimeout))
			return fmt.Errorf("ADX export timed out after %v (operation %s cancelled)", e.cfg.QueryTimeout, executionID)
		}

		body, err := e.mgmt(ctx, "", BuildADXShowOperationCommand(executionID))
		if err != nil {
			return fmt.Errorf("poll ADX operation: %w", err)
		}
		rawState, status, err := ParseADXOperationStatus(body)
		if err != nil {
			return fmt.Errorf("poll ADX operation: %w", err)
		}

		if time.Since(lastLog) >= 30*time.Second {
			e.log.Info("ADX export in progress",
				zap.String("adxOperationId", executionID),
				zap.String("state", rawState),
				zap.Duration("elapsed", time.Since(started)))
			lastLog = time.Now()
		}

		switch MapADXOperationState(rawState) {
		case QueryStateSucceeded:
			e.log.Info("ADX export succeeded",
				zap.String("adxOperationId", executionID),
				zap.Duration("elapsed", time.Since(started)))
			return nil
		case QueryStateFailed:
			return fmt.Errorf("ADX export failed (state=%s): %s", rawState, status)
		case QueryStateCancelled:
			return fmt.Errorf("ADX export was cancelled (state=%s)", rawState)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(adxPollInterval):
		}
	}
}

// GetExecutionResult reports where the export wrote. The blob name prefix is returned as
// the output location; the operation details are read only to log what Kusto produced.
func (e *ADXExecutor) GetExecutionResult(ctx context.Context, executionID string) (*UnloadResult, error) {
	body, err := e.mgmt(ctx, "", BuildADXShowOperationDetailsCommand(executionID))
	if err != nil {
		// Details are advisory: the staged blobs are still discoverable by prefix.
		e.log.Warn("Failed to read ADX operation details", zap.Error(err), zap.String("adxOperationId", executionID))
		return &UnloadResult{OutputLocation: e.cfg.NamePrefix}, nil
	}
	paths, err := ParseADXExportedPaths(body)
	if err != nil {
		e.log.Warn("Failed to parse ADX operation details", zap.Error(err))
	}
	e.log.Info("ADX export output",
		zap.String("adxOperationId", executionID),
		zap.Int("fileCount", len(paths)),
		zap.String("namePrefix", e.cfg.NamePrefix))
	return &UnloadResult{OutputLocation: e.cfg.NamePrefix}, nil
}

// CancelQueryExecution cancels a running export operation.
func (e *ADXExecutor) CancelQueryExecution(ctx context.Context, executionID string) error {
	if _, err := e.mgmt(ctx, "", BuildADXCancelOperationCommand(executionID)); err != nil {
		return fmt.Errorf("cancel ADX operation: %w", err)
	}
	return nil
}

// GetQueryColumns returns the export's column names, used to build the CSV header.
func (e *ADXExecutor) GetQueryColumns(ctx context.Context, query, database string) ([]string, error) {
	body, err := e.query(ctx, database, BuildADXSchemaQuery(query))
	if err != nil {
		return nil, fmt.Errorf("ADX schema query failed: %w", err)
	}
	columns, err := ParseADXSchemaColumns(body)
	if err != nil {
		return nil, fmt.Errorf("ADX schema query failed: %w", err)
	}
	return columns, nil
}

// ---------------------------------------------------------------------------
// Kusto v1 REST transport
// ---------------------------------------------------------------------------

func (e *ADXExecutor) resolveDatabase(database string) string {
	if strings.TrimSpace(database) != "" {
		return strings.TrimSpace(database)
	}
	return strings.TrimSpace(e.cfg.Database)
}

func (e *ADXExecutor) mgmt(ctx context.Context, database, command string) ([]byte, error) {
	return e.do(ctx, "/v1/rest/mgmt", database, command)
}

func (e *ADXExecutor) query(ctx context.Context, database, kql string) ([]byte, error) {
	return e.do(ctx, "/v1/rest/query", database, kql)
}

func (e *ADXExecutor) do(ctx context.Context, path, database, csl string) ([]byte, error) {
	db := e.resolveDatabase(database)
	if db == "" {
		return nil, fmt.Errorf("adx_database is required in connector configuration")
	}
	token, err := e.accessToken(ctx)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]string{"db": db, "csl": csl})
	if err != nil {
		return nil, fmt.Errorf("marshal kusto request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.ClusterURI+path, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build kusto request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-ms-client-request-id", "DatabahnSearchExport;"+uuid.NewString())
	req.Header.Set("x-ms-app", "databahn-jobs")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kusto request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read kusto response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("kusto returned %d: %s", resp.StatusCode, ParseADXError(body))
	}
	return body, nil
}

// accessToken returns a cached AAD token for the cluster, refreshing it a minute early.
//
//nolint:gosec // CWE-532 false positive: the token is used for auth, never logged
func (e *ADXExecutor) accessToken(ctx context.Context) (string, error) {
	if e.token.Token != "" && time.Now().Add(time.Minute).Before(e.token.ExpiresOn) {
		return e.token.Token, nil
	}
	if e.cred == nil {
		return "", fmt.Errorf("ADX executor is not connected")
	}
	token, err := e.cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{e.cfg.ClusterURI + "/.default"},
	})
	if err != nil {
		return "", fmt.Errorf("acquire ADX access token: %w", err)
	}
	e.token = token
	return token.Token, nil
}
