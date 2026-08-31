package query

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
)

// newTestLakeExecutor points a lake executor at a stub endpoint with a pre-seeded token. The
// endpoint is injected into the transport rather than passed through config, which the host
// allowlist would reject.
func newTestLakeExecutor(t *testing.T, handler http.HandlerFunc) *SentinelExecutor {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	exec, err := NewSentinelExecutor(SentinelConfig{
		WorkspaceID:   "ws-guid",
		WorkspaceName: "prod-sentinel",
		StorageTier:   destination.SentinelTierLake,
		MaxRetries:    2,
	})
	if err != nil {
		t.Fatalf(errNewSentinelExecutor, err)
	}
	lake := exec.transport.(*sentinelLakeTransport)
	lake.cfg.Endpoint = srv.URL
	lake.token = azcore.AccessToken{Token: "test-token", ExpiresOn: time.Now().Add(time.Hour)}
	return exec
}

func TestSentinelLakeTransportSendsQuery(t *testing.T) {
	var gotPath, gotAuth, gotCSL, gotDB string
	var gotOptions map[string]interface{}

	exec := newTestLakeExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		var payload map[string]interface{}
		_ = json.Unmarshal(body, &payload)
		gotCSL, _ = payload["csl"].(string)
		gotDB, _ = payload["db"].(string)
		if props, ok := payload["properties"].(map[string]interface{}); ok {
			gotOptions, _ = props["Options"].(map[string]interface{})
		}
		_, _ = w.Write([]byte(lakeV2Response))
	})

	var rows [][]interface{}
	n, err := exec.StreamRows(context.Background(), testSentinelQuery, StreamRowsOptions{},
		func(row []interface{}) error { rows = append(rows, row); return nil })
	if err != nil {
		t.Fatalf(errStreamRows, err)
	}

	if n != 2 || len(rows) != 2 {
		t.Fatalf("rows = %d, collected %d", n, len(rows))
	}
	if gotPath != "/lake/kql/v2/rest/query" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("authorization = %q", gotAuth)
	}
	// The planned KQL runs verbatim, as on the analytics tier.
	if gotCSL != testSentinelQuery {
		t.Fatalf("csl = %q", gotCSL)
	}
	// The workspace is addressed by the composite name, not the GUID.
	if gotDB != "prod-sentinel-ws-guid" {
		t.Fatalf("db = %q", gotDB)
	}
	// The lake API takes the server timeout as an option, not a Prefer header.
	if gotOptions["servertimeout"] != sentinelLakeServerTimeout {
		t.Fatalf("options = %v", gotOptions)
	}
}

func TestSentinelLakeTransportRetriesThrottling(t *testing.T) {
	var attempts int
	exec := newTestLakeExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"throttled"}}`))
			return
		}
		_, _ = w.Write([]byte(lakeV2Response))
	})

	n, err := exec.StreamRows(context.Background(), testSentinelQuery, StreamRowsOptions{},
		func([]interface{}) error { return nil })
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

func TestSentinelLakeTransportDoesNotRetryClientErrors(t *testing.T) {
	var attempts int
	exec := newTestLakeExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"message":"workspace is not tiered to lake"}}`))
	})

	_, err := exec.StreamRows(context.Background(), testSentinelQuery, StreamRowsOptions{},
		func([]interface{}) error { return nil })
	if err == nil {
		t.Fatal("expected 403 to fail")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want no retry on 403", attempts)
	}
	if !strings.Contains(err.Error(), "not tiered to lake") {
		t.Fatalf("error = %v", err)
	}
}

// The row budget applies to the lake tier exactly as it does to analytics.
func TestSentinelLakeTransportEnforcesRowBudget(t *testing.T) {
	exec := newTestLakeExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(lakeV2Response))
	})
	var emitted int
	n, err := exec.StreamRows(context.Background(), testSentinelQuery, StreamRowsOptions{MaxRows: 1},
		func([]interface{}) error { emitted++; return nil })
	if err != nil {
		t.Fatalf("hitting the budget is not an error: %v", err)
	}
	if n != 1 || emitted != 1 {
		t.Fatalf("rows = %d, emitted = %d, want 1", n, emitted)
	}
}
