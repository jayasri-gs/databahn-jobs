package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	// logAnalyticsEndpoint is the Azure public-cloud Log Analytics query host. Sovereign
	// clouds override it via SentinelConfig.Endpoint; the Entra scope is derived from it.
	logAnalyticsEndpoint = "https://api.loganalytics.io"

	// logAnalyticsMaxWaitSeconds is the highest value the Prefer header accepts. There is
	// no way to ask Log Analytics for longer than ten minutes.
	logAnalyticsMaxWaitSeconds = 600

	sentinelMaxErrorBodyBytes = 64 << 10
)

// sentinelTransport is the per-tier seam. The analytics tier (Log Analytics) is implemented
// here; the lake tier is a second implementation over api.securityplatform.microsoft.com
// with a Kusto v2 frame parser, and nothing above this interface changes when it lands.
type sentinelTransport interface {
	Tier() string
	Connect(ctx context.Context) error
	Query(ctx context.Context, kql string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error)
}

// retryableStatusError carries the delay a retryable response asked for.
type retryableStatusError struct {
	status int
	delay  time.Duration
	msg    string
}

func (e *retryableStatusError) Error() string {
	return fmt.Sprintf("Log Analytics returned %d: %s", e.status, e.msg)
}

type logAnalyticsTransport struct {
	cfg        SentinelConfig
	httpClient *http.Client
	cred       *azidentity.ClientSecretCredential
	token      azcore.AccessToken
	log        *zap.Logger
}

func newLogAnalyticsTransport(cfg SentinelConfig, log *zap.Logger) *logAnalyticsTransport {
	return &logAnalyticsTransport{
		cfg: cfg,
		// No client-level timeout: the per-request context carries QueryTimeout, which must
		// be able to reach the ten-minute service ceiling.
		httpClient: &http.Client{},
		log:        log,
	}
}

func (t *logAnalyticsTransport) Tier() string { return SentinelTierAnalytics }

// Connect builds the Entra credential and acquires a first token, which doubles as a
// credential check before any export work starts.
func (t *logAnalyticsTransport) Connect(ctx context.Context) error {
	cred, err := azidentity.NewClientSecretCredential(t.cfg.TenantID, t.cfg.ClientID, t.cfg.ClientSecret, nil)
	if err != nil {
		return fmt.Errorf("create Sentinel service principal credential: %w", err)
	}
	t.cred = cred
	if _, err := t.accessToken(ctx); err != nil {
		return err
	}
	return nil
}

func (t *logAnalyticsTransport) Query(ctx context.Context, kql string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
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
		t.log.Warn("Log Analytics query throttled, retrying",
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

func (t *logAnalyticsTransport) attempt(ctx context.Context, kql string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, error) {
	token, err := t.accessToken(ctx)
	if err != nil {
		return 0, err
	}

	// No "timespan" is sent: the KQL arrives from backend-service with its time filter
	// already applied, and a server-side timespan makes Log Analytics order on the time
	// column, which fails when a projection has dropped it.
	payload, err := json.Marshal(map[string]string{"query": kql})
	if err != nil {
		return 0, fmt.Errorf("marshal Log Analytics request: %w", err)
	}

	timeout := t.cfg.QueryTimeout
	if timeout <= 0 {
		timeout = logAnalyticsMaxWaitSeconds * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, t.queryURL(), bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("build Log Analytics request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Prefer", fmt.Sprintf("wait=%d", preferWaitSeconds(timeout)))
	req.Header.Set("x-ms-client-request-id", "DatabahnSearchExport;"+uuid.NewString())
	req.Header.Set("x-ms-app", "databahn-jobs")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Log Analytics request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, sentinelMaxErrorBodyBytes))
		msg := ParseLogAnalyticsError(body)
		if isRetryableStatus(resp.StatusCode) {
			return 0, &retryableStatusError{
				status: resp.StatusCode,
				delay:  parseRetryAfter(resp.Header.Get("Retry-After")),
				msg:    msg,
			}
		}
		return 0, fmt.Errorf("Log Analytics returned %d: %s", resp.StatusCode, msg)
	}

	return decodeLogAnalyticsResponse(resp.Body, onColumns, onRow)
}

func (t *logAnalyticsTransport) queryURL() string {
	base := strings.TrimSuffix(strings.TrimSpace(t.cfg.Endpoint), "/")
	if base == "" {
		base = logAnalyticsEndpoint
	}
	return fmt.Sprintf("%s/v1/workspaces/%s/query", base, t.cfg.WorkspaceID)
}

// accessToken returns a cached Entra token for the query API, refreshing it a minute early.
//
//nolint:gosec // CWE-532 false positive: the token is used for auth, never logged
func (t *logAnalyticsTransport) accessToken(ctx context.Context) (string, error) {
	if t.token.Token != "" && time.Now().Add(time.Minute).Before(t.token.ExpiresOn) {
		return t.token.Token, nil
	}
	if t.cred == nil {
		return "", fmt.Errorf("Sentinel executor is not connected")
	}
	token, err := t.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{t.scope()}})
	if err != nil {
		return "", fmt.Errorf("acquire Log Analytics access token: %w", err)
	}
	t.token = token
	return token.Token, nil
}

func (t *logAnalyticsTransport) scope() string {
	base := strings.TrimSuffix(strings.TrimSpace(t.cfg.Endpoint), "/")
	if base == "" {
		base = logAnalyticsEndpoint
	}
	return base + "/.default"
}

// preferWaitSeconds clamps the requested wait to the service ceiling.
func preferWaitSeconds(timeout time.Duration) int {
	seconds := int(timeout / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	if seconds > logAnalyticsMaxWaitSeconds {
		seconds = logAnalyticsMaxWaitSeconds
	}
	return seconds
}

func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusBadGateway:
		return true
	}
	return false
}

// sentinelFallbackRetryDelay mirrors the backend's fallback ladder for throttled Sentinel
// requests when the service omits Retry-After: 10s, 30s, 60s.
func sentinelFallbackRetryDelay(attempt int) time.Duration {
	ladder := []time.Duration{10 * time.Second, 30 * time.Second, 60 * time.Second}
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(ladder) {
		attempt = len(ladder) - 1
	}
	return ladder[attempt]
}

// parseRetryAfter accepts both forms the header can take: delay-seconds and an HTTP date.
func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return clampRetryDelay(time.Duration(seconds) * time.Second)
	}
	if when, err := http.ParseTime(value); err == nil {
		return clampRetryDelay(time.Until(when))
	}
	return 0
}

func clampRetryDelay(d time.Duration) time.Duration {
	const (
		minDelay = time.Second
		maxDelay = 2 * time.Minute
	)
	if d < minDelay {
		return minDelay
	}
	if d > maxDelay {
		return maxDelay
	}
	return d
}

func asRetryable(err error, target **retryableStatusError) bool {
	if r, ok := err.(*retryableStatusError); ok {
		*target = r
		return true
	}
	return false
}
