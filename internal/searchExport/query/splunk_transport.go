package query

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	splunkExportPath          = "/services/search/jobs/export"
	splunkConnectPath         = "/services/server/info"
	splunkMaxErrorBodyBytes   = 64 << 10
	splunkReservedHostMessage = "splunk_host must not target private or reserved network addresses"
)

// splunkResolveHost resolves a Splunk search host for SSRF validation and DNS pinning.
// Tests may replace this hook to aim transports at httptest servers.
var splunkResolveHost = resolveSplunkHostIPs

type splunkHTTPTransport struct {
	cfg        SplunkConfig
	host       string
	pinnedIP   net.IP
	httpClient *http.Client
	log        *zap.Logger
}

func newSplunkTransport(cfg SplunkConfig, log *zap.Logger) (*splunkHTTPTransport, error) {
	if log == nil {
		log = zap.NewNop()
	}
	host := strings.TrimSpace(cfg.Host)
	ips, err := splunkResolveHost(host)
	if err != nil {
		return nil, fmt.Errorf("resolve splunk_host %q: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolve splunk_host %q: no addresses", host)
	}
	pinnedIP, err := selectSplunkPinnedIP(ips)
	if err != nil {
		return nil, err
	}

	t := &splunkHTTPTransport{
		cfg:      cfg,
		host:     host,
		pinnedIP: pinnedIP,
		log:      log,
	}
	t.httpClient = &http.Client{
		Transport: t.buildRoundTripper(),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("splunk export: redirects are not allowed")
		},
	}
	return t, nil
}

func (t *splunkHTTPTransport) buildRoundTripper() http.RoundTripper {
	port := t.cfg.Port
	if port <= 0 {
		port = defaultSplunkPort
	}
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			_, addrPort, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			pinnedAddr := net.JoinHostPort(t.pinnedIP.String(), addrPort)
			return dialer.DialContext(ctx, network, pinnedAddr)
		},
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if strings.EqualFold(t.cfg.Scheme, "https") {
		transport.TLSClientConfig = &tls.Config{
			ServerName:         t.host,
			InsecureSkipVerify: !t.cfg.SSLCertValidation,
			MinVersion:         tls.VersionTLS12,
		}
	}
	return transport
}

func (t *splunkHTTPTransport) Connect(ctx context.Context) error {
	req, err := t.newRequest(ctx, http.MethodGet, splunkConnectPath, nil)
	if err != nil {
		return err
	}
	resp, err := t.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("splunk credential check failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, splunkMaxErrorBodyBytes))
		msg := ParseSplunkError(body)
		return fmt.Errorf("splunk credential check returned %d: %s", resp.StatusCode, msg)
	}
	return nil
}

func (t *splunkHTTPTransport) Export(ctx context.Context, spl string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, SplunkStreamMetadata, error) {
	maxRetries := t.cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	var rows int64
	var meta SplunkStreamMetadata
	for attempt := 0; ; attempt++ {
		var err error
		rows, meta, err = t.attempt(ctx, spl, onColumns, onRow)
		if err == nil {
			return rows, meta, nil
		}
		var retryable *splunkRetryableStatusError
		if !asSplunkRetryable(err, &retryable) || attempt >= maxRetries {
			return rows, meta, err
		}
		delay := retryable.delay
		if delay <= 0 {
			delay = splunkFallbackRetryDelay(attempt)
		}
		t.log.Warn("Splunk export throttled, retrying",
			zap.Int("status", retryable.status),
			zap.Int("attempt", attempt+1),
			zap.Int("maxRetries", maxRetries),
			zap.Duration("delay", delay))
		select {
		case <-ctx.Done():
			return rows, meta, ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (t *splunkHTTPTransport) attempt(ctx context.Context, spl string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, SplunkStreamMetadata, error) {
	form := url.Values{}
	form.Set("search", spl)
	form.Set("output_mode", "json_rows")
	body := strings.NewReader(form.Encode())

	timeout := t.cfg.QueryTimeout
	if timeout <= 0 {
		timeout = defaultSplunkQueryTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
	defer cancel()

	req, err := t.newRequest(reqCtx, http.MethodPost, splunkExportPath, body)
	if err != nil {
		return 0, SplunkStreamMetadata{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return 0, SplunkStreamMetadata{}, fmt.Errorf("splunk export request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		rawBody, _ := io.ReadAll(io.LimitReader(resp.Body, splunkMaxErrorBodyBytes))
		msg := ParseSplunkError(rawBody)
		if isSplunkRetryableStatus(resp.StatusCode) || isSplunkConcurrencyQuotaMessage(msg) {
			return 0, SplunkStreamMetadata{}, &splunkRetryableStatusError{
				status: resp.StatusCode,
				delay:  parseRetryAfter(resp.Header.Get("Retry-After")),
				msg:    msg,
			}
		}
		return 0, SplunkStreamMetadata{}, fmt.Errorf("splunk export returned %d: %s", resp.StatusCode, msg)
	}

	return decodeSplunkJsonRowsResponseWithMetadata(resp.Body, onColumns, onRow)
}

func (t *splunkHTTPTransport) exportURL(path string) string {
	port := t.cfg.Port
	if port <= 0 {
		port = defaultSplunkPort
	}
	return fmt.Sprintf("%s://%s:%d%s", strings.ToLower(t.cfg.Scheme), t.host, port, path)
}

func (t *splunkHTTPTransport) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, t.exportURL(path), body)
	if err != nil {
		return nil, fmt.Errorf("build splunk request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+t.cfg.Token)
	return req, nil
}

type splunkRetryableStatusError struct {
	status int
	delay  time.Duration
	msg    string
}

func (e *splunkRetryableStatusError) Error() string {
	return fmt.Sprintf("Splunk returned %d: %s", e.status, e.msg)
}

func isSplunkRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return true
	}
	return false
}

func isSplunkConcurrencyQuotaMessage(msg string) bool {
	return strings.Contains(strings.ToLower(msg), "maximum number of concurrent historical searches reached")
}

func splunkFallbackRetryDelay(attempt int) time.Duration {
	ladder := []time.Duration{10 * time.Second, 30 * time.Second, 60 * time.Second}
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(ladder) {
		attempt = len(ladder) - 1
	}
	return ladder[attempt]
}

func asSplunkRetryable(err error, target **splunkRetryableStatusError) bool {
	if r, ok := err.(*splunkRetryableStatusError); ok {
		*target = r
		return true
	}
	return false
}

func resolveSplunkHostIPs(host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		if err := validateSplunkResolvedIP(ip); err != nil {
			return nil, err
		}
		return []net.IP{ip}, nil
	}
	return net.LookupIP(host)
}

func selectSplunkPinnedIP(ips []net.IP) (net.IP, error) {
	var lastErr error
	for _, ip := range ips {
		if ip == nil {
			continue
		}
		normalized := ip.To4()
		if normalized == nil {
			normalized = ip.To16()
		}
		if err := validateSplunkResolvedIP(normalized); err != nil {
			lastErr = err
			continue
		}
		return normalized, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("%s", splunkReservedHostMessage)
}

func validateSplunkResolvedIP(ip net.IP) error {
	if ip == nil || ip.IsUnspecified() {
		return fmt.Errorf("%s", splunkReservedHostMessage)
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsPrivate() {
		return fmt.Errorf("%s", splunkReservedHostMessage)
	}
	return nil
}

// ParseSplunkError extracts a readable message from a non-2xx Splunk response body.
func ParseSplunkError(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "empty response"
	}
	if len(trimmed) > 512 {
		trimmed = trimmed[:512] + "…"
	}
	return trimmed
}
