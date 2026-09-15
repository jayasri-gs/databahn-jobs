package query

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func testSplunkConfig() SplunkConfig {
	return SplunkConfig{
		Host:              "splunk.test.local",
		Port:              8089,
		Scheme:            "http",
		Token:             "test-token",
		SSLCertValidation: true,
		QueryTimeout:      time.Minute,
		MaxRetries:        2,
	}
}

func attachTestSplunkTransport(t *testing.T, srv *httptest.Server, cfg SplunkConfig) *splunkHTTPTransport {
	t.Helper()
	host, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split test server addr: %v", err)
	}
	pinnedIP := net.ParseIP(host)
	if pinnedIP == nil {
		t.Fatalf("parse pinned IP from %q", host)
	}
	if cfg.Token == "" {
		cfg.Token = "test-token"
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if u, parseErr := url.Parse(srv.URL); parseErr == nil && u.Port() != "" {
		if p, convErr := strconv.Atoi(u.Port()); convErr == nil {
			cfg.Port = p
		}
	} else if portStr != "" {
		if p, convErr := strconv.Atoi(portStr); convErr == nil {
			cfg.Port = p
		}
	}

	transport := &splunkHTTPTransport{
		cfg:      cfg,
		host:     cfg.Host,
		pinnedIP: pinnedIP,
		log:      zap.NewNop(),
	}
	transport.httpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("splunk export: redirects are not allowed")
		},
	}
	return transport
}

func TestNewSplunkTransportRejectsLoopbackHost(t *testing.T) {
	_, err := newSplunkTransport(SplunkConfig{
		Host:   "127.0.0.1",
		Token:  "secret",
		Port:   8089,
		Scheme: "https",
	}, nil)
	if err == nil {
		t.Fatal("expected loopback host to be rejected")
	}
	if !strings.Contains(err.Error(), splunkReservedHostMessage) {
		t.Fatalf("error = %q", err)
	}
}

func TestValidateSplunkResolvedIPRejectsPrivateAddresses(t *testing.T) {
	for _, ipStr := range []string{"127.0.0.1", "10.0.0.1", "169.254.1.1", "224.0.0.1", "fc00::1"} {
		if err := validateSplunkResolvedIP(net.ParseIP(ipStr)); err == nil {
			t.Fatalf("expected %s to be rejected", ipStr)
		}
	}
}

func TestSplunkTransportConnectChecksCredentials(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	transport := attachTestSplunkTransport(t, srv, testSplunkConfig())
	if err := transport.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if gotMethod != http.MethodGet {
		t.Fatalf("method = %q", gotMethod)
	}
	if gotPath != splunkConnectPath {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("authorization = %q", gotAuth)
	}
}

func TestSplunkTransportSendsExportRequest(t *testing.T) {
	var gotMethod, gotPath, gotAuth, gotSearch, gotOutputMode string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		values, _ := url.ParseQuery(string(body))
		gotSearch = values.Get("search")
		gotOutputMode = values.Get("output_mode")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	transport := attachTestSplunkTransport(t, srv, testSplunkConfig())
	n, _, err := transport.Export(context.Background(), "search index=main | head 1", nil, nil)
	if err != nil {
		t.Fatalf("Export err = %v", err)
	}
	if n != 0 {
		t.Fatalf("rows = %d, want 0 for empty response body", n)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q", gotMethod)
	}
	if gotPath != splunkExportPath {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("authorization = %q", gotAuth)
	}
	if gotSearch != "search index=main | head 1" {
		t.Fatalf("search = %q", gotSearch)
	}
	if gotOutputMode != "json_rows" {
		t.Fatalf("output_mode = %q", gotOutputMode)
	}
}

func TestSplunkTransportRetriesOn429(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	transport := attachTestSplunkTransport(t, srv, testSplunkConfig())
	n, _, err := transport.Export(context.Background(), "search index=main", nil, nil)
	if err != nil {
		t.Fatalf("Export err = %v", err)
	}
	if n != 0 {
		t.Fatalf("rows = %d, want 0 for empty response body", n)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestSplunkTransportRejectsRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.com/", http.StatusFound)
	}))
	t.Cleanup(srv.Close)

	transport := attachTestSplunkTransport(t, srv, testSplunkConfig())
	_, _, err := transport.Export(context.Background(), "search index=main", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "redirects are not allowed") {
		t.Fatalf("Export err = %v", err)
	}
}
