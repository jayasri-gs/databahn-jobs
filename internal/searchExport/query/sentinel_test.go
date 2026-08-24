package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

// newTestSentinelExecutor points an executor at a stub Log Analytics endpoint with a
// pre-seeded token, so the REST layer runs without Entra.
func newTestSentinelExecutor(t *testing.T, handler http.HandlerFunc) *SentinelExecutor {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	// The endpoint is checked against an allowlist of Azure Monitor Logs hosts, so the stub
	// server is injected into the transport rather than passed through config as an
	// arbitrary URL.
	exec, err := NewSentinelExecutor(SentinelConfig{
		WorkspaceID: "ws-guid",
		MaxRetries:  2,
	})
	if err != nil {
		t.Fatalf(errNewSentinelExecutor, err)
	}
	transport := exec.transport.(*logAnalyticsTransport)
	transport.cfg.Endpoint = srv.URL
	transport.token = azcore.AccessToken{Token: "test-token", ExpiresOn: time.Now().Add(time.Hour)}
	return exec
}

const sentinelTwoRowResponse = `{"tables":[{"name":"PrimaryResult",` +
	`"columns":[{"name":"TimeGenerated","type":"datetime"},{"name":"Account","type":"string"}],` +
	`"rows":[["2026-08-20T10:00:00Z","alice"],["2026-08-20T11:00:00Z","bob"]]}]}`

func TestSentinelExecutorSendsQueryRequest(t *testing.T) {
	var gotPath, gotAuth, gotPrefer, gotQuery string
	var gotBody map[string]interface{}
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotPrefer = r.Header.Get("Prefer")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		gotQuery, _ = gotBody["query"].(string)
		_, _ = w.Write([]byte(sentinelTwoRowResponse))
	})

	var columns []string
	var rows [][]interface{}
	opts := StreamRowsOptions{OnColumns: func(cols []string) error { columns = cols; return nil }}
	n, err := exec.StreamRows(context.Background(), testSentinelQuery, opts, func(row []interface{}) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf(errStreamRows, err)
	}

	if n != 2 || len(rows) != 2 {
		t.Fatalf("rows = %d, collected %d", n, len(rows))
	}
	if gotPath != "/v1/workspaces/ws-guid/query" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("authorization = %q", gotAuth)
	}
	if gotPrefer != "wait=600" {
		t.Fatalf("prefer = %q, want the ten-minute service ceiling", gotPrefer)
	}
	// The KQL arrives fully planned and must run verbatim.
	if gotQuery != testSentinelQuery {
		t.Fatalf("query = %q", gotQuery)
	}
	// No timespan: the time filter is already in the query, and a server-side timespan
	// makes Log Analytics order on a column a projection may have dropped.
	if _, ok := gotBody["timespan"]; ok {
		t.Fatalf("request should not carry a timespan: %v", gotBody)
	}
	if !sameColumns(columns, []string{"TimeGenerated", "Account"}) {
		t.Fatalf(gotColumns, columns)
	}
	if rows[1][1] != "bob" {
		t.Fatalf("row = %v", rows[1])
	}
}

func TestSentinelExecutorRunsOneQueryPerExport(t *testing.T) {
	var requests int
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(sentinelTwoRowResponse))
	})
	if _, err := exec.StreamRows(context.Background(), "SecurityEvent", StreamRowsOptions{}, func([]interface{}) error { return nil }); err != nil {
		t.Fatalf(errStreamRows, err)
	}
	if requests != 1 {
		t.Fatalf("issued %d requests, want 1 — chunking is deliberately not enabled", requests)
	}
}

func TestSentinelExecutorPartialResultsFailExport(t *testing.T) {
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"tables":[{"name":"PrimaryResult","columns":[{"name":"A","type":"string"}],"rows":[["one"]]}],` +
			`"error":{"code":"PartialError","message":"result set exceeded the limit"}}`))
	})
	_, err := exec.StreamRows(context.Background(), "SecurityEvent", StreamRowsOptions{}, func([]interface{}) error { return nil })
	if err == nil {
		t.Fatal("partial results must fail the export, never truncate silently")
	}
	if !strings.Contains(err.Error(), "Narrow the time range") {
		t.Fatalf("error = %q, want actionable guidance", err)
	}
}

func TestSentinelExecutorRetriesThrottling(t *testing.T) {
	var attempts int
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"TooManyRequests","message":"rate limited"}}`))
			return
		}
		_, _ = w.Write([]byte(sentinelTwoRowResponse))
	})

	n, err := exec.StreamRows(context.Background(), "SecurityEvent", StreamRowsOptions{}, func([]interface{}) error { return nil })
	if err != nil {
		t.Fatalf(errStreamRows, err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want a retry after 429", attempts)
	}
	if n != 2 {
		t.Fatalf("rows = %d", n)
	}
}

func TestSentinelExecutorGivesUpAfterMaxRetries(t *testing.T) {
	var attempts int
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	if _, err := exec.StreamRows(context.Background(), "SecurityEvent", StreamRowsOptions{}, func([]interface{}) error { return nil }); err == nil {
		t.Fatal("expected failure after exhausting retries")
	}
	// MaxRetries=2 means three attempts in total.
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestSentinelExecutorDoesNotRetryClientErrors(t *testing.T) {
	var attempts int
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"missing Log Analytics Reader"}}`))
	})
	_, err := exec.StreamRows(context.Background(), "SecurityEvent", StreamRowsOptions{}, func([]interface{}) error { return nil })
	if err == nil {
		t.Fatal("expected 403 to fail")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want no retry on 403", attempts)
	}
	if !strings.Contains(err.Error(), "missing Log Analytics Reader") {
		t.Fatalf("error = %q", err)
	}
}

func TestSentinelExecutorEnforcesRowBudget(t *testing.T) {
	rows := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		rows = append(rows, fmt.Sprintf(`["row-%d"]`, i))
	}
	body := `{"tables":[{"name":"PrimaryResult","columns":[{"name":"A","type":"string"}],"rows":[` +
		strings.Join(rows, ",") + `]}]}`
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	})

	var emitted int
	n, err := exec.StreamRows(context.Background(), "SecurityEvent", StreamRowsOptions{MaxRows: 3}, func([]interface{}) error {
		emitted++
		return nil
	})
	if err != nil {
		t.Fatalf("hitting the budget is not an error: %v", err)
	}
	if n != 3 || emitted != 3 {
		t.Fatalf("rows = %d, emitted = %d, want 3", n, emitted)
	}
}

func TestSentinelExecutorEmptyResultStillResolvesColumns(t *testing.T) {
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"tables":[{"name":"PrimaryResult","columns":[{"name":"A","type":"string"}],"rows":[]}]}`))
	})
	var columns []string
	opts := StreamRowsOptions{OnColumns: func(cols []string) error { columns = cols; return nil }}
	n, err := exec.StreamRows(context.Background(), "SecurityEvent", opts, func([]interface{}) error { return nil })
	if err != nil {
		t.Fatalf(errStreamRows, err)
	}
	if n != 0 {
		t.Fatalf("rows = %d", n)
	}
	// An empty export must still write a header rather than fail for missing columns.
	if !sameColumns(columns, []string{"A"}) {
		t.Fatalf(gotColumns, columns)
	}
}

func TestSentinelExecutorGetQueryColumnsUsesTakeZero(t *testing.T) {
	var gotQuery string
	exec := newTestSentinelExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]string
		_ = json.Unmarshal(body, &payload)
		gotQuery = payload["query"]
		_, _ = w.Write([]byte(`{"tables":[{"name":"PrimaryResult","columns":[{"name":"A","type":"string"}],"rows":[]}]}`))
	})
	cols, err := exec.GetQueryColumns(context.Background(), "SecurityEvent | take 100", "")
	if err != nil {
		t.Fatalf("GetQueryColumns: %v", err)
	}
	if !sameColumns(cols, []string{"A"}) {
		t.Fatalf(gotColumns, cols)
	}
	if !strings.HasSuffix(gotQuery, "| take 0") {
		t.Fatalf("query = %q, want a schema-only probe", gotQuery)
	}
}

func TestNewSentinelExecutorRejectsLakeTier(t *testing.T) {
	_, err := NewSentinelExecutor(SentinelConfig{WorkspaceID: "ws", StorageTier: destination.SentinelTierLake})
	if err == nil {
		t.Fatal("lake tier should be rejected until it has a transport")
	}
	if !strings.Contains(err.Error(), "storage_tier=LAKE") {
		t.Fatalf("error = %q, want the tier named", err)
	}
}

func TestNewSentinelExecutorDefaults(t *testing.T) {
	exec, err := NewSentinelExecutor(SentinelConfig{WorkspaceID: "ws"})
	if err != nil {
		t.Fatalf(errNewSentinelExecutor, err)
	}
	if exec.Engine() != EngineSentinelLAW {
		t.Fatalf("engine = %q", exec.Engine())
	}
	// A blank tier is the analytics tier, matching backend-service.
	if exec.transport.Tier() != SentinelTierAnalytics {
		t.Fatalf("tier = %q", exec.transport.Tier())
	}
	if exec.cfg.QueryTimeout != defaultSentinelQueryTimeout {
		t.Fatalf("queryTimeout = %v", exec.cfg.QueryTimeout)
	}
	if _, err := NewSentinelExecutor(SentinelConfig{}); err == nil {
		t.Fatal("missing workspace_id should fail")
	}
}

func TestSentinelExecutorValidateExportQuery(t *testing.T) {
	exec, err := NewSentinelExecutor(SentinelConfig{WorkspaceID: "ws"})
	if err != nil {
		t.Fatalf(errNewSentinelExecutor, err)
	}
	if err := exec.ValidateExportQuery(context.Background(), "  "); err == nil {
		t.Fatal("blank query should be rejected")
	}
	if err := exec.ValidateExportQuery(context.Background(), "SecurityEvent"); err != nil {
		t.Fatalf("ValidateExportQuery: %v", err)
	}
	// Nothing runs server-side, so there is nothing to cancel.
	if err := exec.CancelQueryExecution(context.Background(), "irrelevant"); err != nil {
		t.Fatalf("CancelQueryExecution: %v", err)
	}
}

func TestPlanSentinelQueriesIsSingleRequest(t *testing.T) {
	plans := PlanSentinelQueries("  SecurityEvent | take 10  ")
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	if plans[0].KQL != testSentinelQuery {
		t.Fatalf("kql = %q, want the planned query verbatim", plans[0].KQL)
	}
}

func TestPreferWaitSecondsClamped(t *testing.T) {
	cases := map[time.Duration]int{
		0:                1,
		30 * time.Second: 30,
		time.Hour:        logAnalyticsMaxWaitSeconds,
	}
	for in, want := range cases {
		if got := preferWaitSeconds(in); got != want {
			t.Fatalf("preferWaitSeconds(%v) = %d, want %d", in, got, want)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("30"); got != 30*time.Second {
		t.Fatalf("seconds form = %v", got)
	}
	// Clamped so a hostile or stale header cannot park the worker.
	if got := parseRetryAfter("100000"); got != 2*time.Minute {
		t.Fatalf("clamp = %v", got)
	}
	if got := parseRetryAfter(""); got != 0 {
		t.Fatalf("empty = %v", got)
	}
	if got := parseRetryAfter("not-a-date"); got != 0 {
		t.Fatalf("garbage = %v", got)
	}
	future := time.Now().Add(20 * time.Second).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(future); got < 5*time.Second || got > 25*time.Second {
		t.Fatalf("http-date form = %v", got)
	}
}

func TestSentinelFallbackRetryDelay(t *testing.T) {
	want := []time.Duration{10 * time.Second, 30 * time.Second, 60 * time.Second, 60 * time.Second}
	for i, w := range want {
		if got := sentinelFallbackRetryDelay(i); got != w {
			t.Fatalf("sentinelFallbackRetryDelay(%d) = %v, want %v", i, got, w)
		}
	}
}

func TestLogAnalyticsScopeFollowsEndpoint(t *testing.T) {
	def := newLogAnalyticsTransport(SentinelConfig{WorkspaceID: "ws"}, nil)
	if def.scope() != "https://api.loganalytics.io/.default" {
		t.Fatalf("default scope = %q", def.scope())
	}
	if def.queryURL() != "https://api.loganalytics.io/v1/workspaces/ws/query" {
		t.Fatalf("default url = %q", def.queryURL())
	}
	// Sovereign clouds override the host; the Entra scope must follow it.
	sov := newLogAnalyticsTransport(SentinelConfig{WorkspaceID: "ws", Endpoint: "https://api.loganalytics.us/"}, nil)
	if sov.scope() != "https://api.loganalytics.us/.default" {
		t.Fatalf("sovereign scope = %q", sov.scope())
	}
}

// The endpoint receives an Entra bearer token minted for its own scope, so it is restricted
// to approved Azure Monitor Logs hosts rather than anywhere a configuration names.
func TestNewSentinelExecutorValidatesEndpoint(t *testing.T) {
	valid := []string{
		"",
		"https://api.loganalytics.io",
		"https://api.loganalytics.io/",
		"https://api.loganalytics.us",
		"https://api.loganalytics.azure.cn",
	}
	for _, endpoint := range valid {
		if _, err := NewSentinelExecutor(SentinelConfig{WorkspaceID: "ws", Endpoint: endpoint}); err != nil {
			t.Fatalf("endpoint %q rejected: %v", endpoint, err)
		}
	}

	invalid := []struct{ name, endpoint string }{
		{"plain http", "http://api.loganalytics.io"},
		{"attacker host", "https://attacker.example.com"},
		{"approved host as a subdomain", "https://api.loganalytics.io.attacker.example.com"},
		{"loopback", "https://127.0.0.1:8080"},
		{"loopback name", "https://localhost"},
		{"link-local metadata", "https://169.254.169.254"},
		{"embedded credentials", "https://user:pass@api.loganalytics.io"},
		{"not a url", "://"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSentinelExecutor(SentinelConfig{WorkspaceID: "ws", Endpoint: tc.endpoint}); err == nil {
				t.Fatalf("endpoint %q was accepted", tc.endpoint)
			}
		})
	}
}
