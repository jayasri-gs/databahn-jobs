package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	// sentinelLakeEndpoint is the Sentinel data lake KQL host. Mirrors backend-service
	// AzureConstants.SENTINEL_LAKE_KQL_QUERY_URL.
	sentinelLakeEndpoint = "https://api.securityplatform.microsoft.com"
	sentinelLakeQueryURL = sentinelLakeEndpoint + "/lake/kql/v2/rest/query"

	// sentinelLakeScope is the Entra scope for the lake API — a resource GUID rather than a
	// hostname, so unlike Log Analytics it cannot be derived from the endpoint.
	sentinelLakeScope = "4500ebfb-89b6-4b14-a480-7f749797bfcd/.default"

	// sentinelLakeServerTimeout is the servertimeout option backend-service sends. The lake
	// API takes it in the request body rather than a Prefer header.
	sentinelLakeServerTimeout = "00:04:00"
)

// retryAfterMessage matches the "retry after <timestamp> UTC" the lake API puts in a 429 body
// when it omits the Retry-After header. Mirrors backend-service
// SentinelLakeKqlSupport.RETRY_AFTER_MESSAGE_PATTERN.
var retryAfterMessage = regexp.MustCompile(`(?i)retry after\s+(.+?)\s+UTC`)

const retryAfterMessageLayout = "01/02/2006 15:04:05"

type sentinelLakeTransport struct {
	cfg        SentinelConfig
	database   string
	httpClient *http.Client
	cred       *azidentity.ClientSecretCredential
	token      azcore.AccessToken
	log        *zap.Logger
}

func newSentinelLakeTransport(cfg SentinelConfig, database string, log *zap.Logger) *sentinelLakeTransport {
	return &sentinelLakeTransport{
		cfg:      cfg,
		database: database,
		// No client-level timeout: the per-request context carries QueryTimeout.
		httpClient: &http.Client{},
		log:        log,
	}
}

func (t *sentinelLakeTransport) Tier() string { return SentinelTierLake }

func (t *sentinelLakeTransport) Connect(ctx context.Context) error {
	cred, err := azidentity.NewClientSecretCredential(t.cfg.TenantID, t.cfg.ClientID, t.cfg.ClientSecret, nil)
	if err != nil {
		return fmt.Errorf("create Sentinel lake service principal credential: %w", err)
	}
	t.cred = cred
	if _, err := t.accessToken(ctx); err != nil {
		return err
	}
	return nil
}

func (t *sentinelLakeTransport) Query(ctx context.Context, kql string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	maxRetries := t.cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := 0; ; attempt++ {
		rows, err := t.attempt(ctx, kql, onColumns, onRow)
		if err == nil {
			return rows, nil
		}
		var retryable *retryableStatusError
		if !asRetryable(err, &retryable) || attempt >= maxRetries {
			return rows, err
		}
		delay := retryable.delay
		if delay <= 0 {
			delay = sentinelFallbackRetryDelay(attempt)
		}
		t.log.Warn("Sentinel lake query throttled, retrying",
			zap.Int("status", retryable.status),
			zap.Int("attempt", attempt+1),
			zap.Int("maxRetries", maxRetries),
			zap.Duration("delay", delay))
		select {
		case <-ctx.Done():
			return rows, ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (t *sentinelLakeTransport) attempt(ctx context.Context, kql string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	token, err := t.accessToken(ctx)
	if err != nil {
		return 0, err
	}

	// The lake API takes the workspace as a composite db field in the body, and the server
	// timeout as a query option — there is no Prefer header and no workspace in the path.
	payload, err := json.Marshal(map[string]interface{}{
		"csl": kql,
		"db":  t.database,
		"properties": map[string]interface{}{
			"Options": map[string]interface{}{
				"servertimeout":    sentinelLakeServerTimeout,
				"queryconsistency": "strongconsistency",
				"query_language":   "kql",
			},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("marshal Sentinel lake request: %w", err)
	}

	timeout := t.cfg.QueryTimeout
	if timeout <= 0 {
		timeout = defaultSentinelQueryTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, t.queryURL(), bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("build Sentinel lake request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-ms-client-request-id", "DatabahnSearchExport;"+uuid.NewString())
	req.Header.Set("x-ms-app", "databahn-jobs")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Sentinel lake request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, sentinelMaxErrorBodyBytes))
		msg := ParseLogAnalyticsError(body)
		if isRetryableStatus(resp.StatusCode) {
			return 0, &retryableStatusError{
				status: resp.StatusCode,
				delay:  lakeRetryDelay(resp.Header.Get("Retry-After"), body),
				msg:    msg,
			}
		}
		return 0, fmt.Errorf("Sentinel lake returned %d: %s", resp.StatusCode, msg)
	}

	return decodeSentinelLakeResponse(resp.Body, onColumns, onRow)
}

func (t *sentinelLakeTransport) queryURL() string {
	base := strings.TrimSuffix(strings.TrimSpace(t.cfg.Endpoint), "/")
	if base == "" {
		return sentinelLakeQueryURL
	}
	return base + "/lake/kql/v2/rest/query"
}

// accessToken returns a cached Entra token for the lake API, refreshing it a minute early.
//
//nolint:gosec // CWE-532 false positive: the token is used for auth, never logged
func (t *sentinelLakeTransport) accessToken(ctx context.Context) (string, error) {
	if t.token.Token != "" && time.Now().Add(time.Minute).Before(t.token.ExpiresOn) {
		return t.token.Token, nil
	}
	if t.cred == nil {
		return "", fmt.Errorf("Sentinel lake executor is not connected")
	}
	token, err := t.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{sentinelLakeScope}})
	if err != nil {
		return "", fmt.Errorf("acquire Sentinel lake access token: %w", err)
	}
	t.token = token
	return token.Token, nil
}

// lakeRetryDelay prefers the Retry-After header, then the "retry after … UTC" the lake API
// writes into a 429 body when it omits the header.
func lakeRetryDelay(header string, body []byte) time.Duration {
	if d := parseRetryAfter(header); d > 0 {
		return d
	}
	match := retryAfterMessage.FindSubmatch(body)
	if match == nil {
		return 0
	}
	when, err := time.Parse(retryAfterMessageLayout, strings.TrimSpace(string(match[1])))
	if err != nil {
		return 0
	}
	return clampRetryDelay(time.Until(when.UTC()))
}
