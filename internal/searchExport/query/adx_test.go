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
)

// newTestADXExecutor points an executor at a stub cluster with a pre-seeded token,
// so the REST layer can be exercised without Entra.
func newTestADXExecutor(t *testing.T, handler http.HandlerFunc) (*ADXExecutor, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	exec := NewADXExecutor(ADXConfig{ClusterURI: srv.URL, Database: "SecurityLogs"})
	exec.token = azcore.AccessToken{Token: "test-token", ExpiresOn: time.Now().Add(time.Hour)}
	return exec, srv
}

func TestADXExecutorSendsKustoRequest(t *testing.T) {
	var gotPath, gotAuth, gotContentType, gotDB, gotCSL string
	exec, _ := newTestADXExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		var payload map[string]string
		_ = json.Unmarshal(body, &payload)
		gotDB, gotCSL = payload["db"], payload["csl"]
		_, _ = w.Write([]byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"State"},{"ColumnName":"Status"}],"Rows":[["Completed",""]]}]}`))
	})

	status, err := exec.CheckQueryStatus(context.Background(), "op-1")
	if err != nil {
		t.Fatalf("CheckQueryStatus: %v", err)
	}
	if status != QueryStateSucceeded {
		t.Fatalf("status = %q", status)
	}
	if gotPath != "/v1/rest/mgmt" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("authorization = %q", gotAuth)
	}
	if !strings.HasPrefix(gotContentType, "application/json") {
		t.Fatalf("content type = %q", gotContentType)
	}
	if gotDB != "SecurityLogs" {
		t.Fatalf("db = %q", gotDB)
	}
	if gotCSL != `.show operations "op-1"` {
		t.Fatalf("csl = %q", gotCSL)
	}
}

func TestADXExecutorSurfacesClusterErrors(t *testing.T) {
	exec, _ := newTestADXExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"BadRequest","message":"outer","@innererror":{"message":"Semantic error: unknown table"}}}`))
	})
	_, err := exec.CheckQueryStatus(context.Background(), "op-1")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "Semantic error: unknown table") || !strings.Contains(err.Error(), "400") {
		t.Fatalf("error = %v", err)
	}
}

func TestADXGetQueryColumnsUsesQueryEndpoint(t *testing.T) {
	var gotPath, gotCSL string
	exec, _ := newTestADXExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		var payload map[string]string
		_ = json.Unmarshal(body, &payload)
		gotCSL = payload["csl"]
		_, _ = w.Write([]byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"ColumnName"}],"Rows":[["Timestamp"],["Message"]]}]}`))
	})

	cols, err := exec.GetQueryColumns(context.Background(), "Logs | take 5", "")
	if err != nil {
		t.Fatalf("GetQueryColumns: %v", err)
	}
	if len(cols) != 2 || cols[0] != "Timestamp" {
		t.Fatalf(gotColumns, cols)
	}
	if gotPath != "/v1/rest/query" {
		t.Fatalf("path = %q", gotPath)
	}
	if !strings.Contains(gotCSL, "| getschema") {
		t.Fatalf("csl = %q", gotCSL)
	}
}

func TestADXRequestDatabaseFallback(t *testing.T) {
	var gotDB string
	exec, _ := newTestADXExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]string
		_ = json.Unmarshal(body, &payload)
		gotDB = payload["db"]
		_, _ = w.Write([]byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"ColumnName"}],"Rows":[]}]}`))
	})

	if _, err := exec.GetQueryColumns(context.Background(), "Logs", "OverrideDb"); err != nil {
		t.Fatalf("GetQueryColumns: %v", err)
	}
	if gotDB != "OverrideDb" {
		t.Fatalf("request database = %q, want the override database %q to win", gotDB, "OverrideDb")
	}

	exec.cfg.Database = ""
	if _, err := exec.GetQueryColumns(context.Background(), "Logs", ""); err == nil {
		t.Fatal("expected error when no database is configured")
	}
}

func TestADXGetExecutionResultReturnsNamePrefix(t *testing.T) {
	exec, _ := newTestADXExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"Path"}],"Rows":[["https://acct.blob.core.windows.net/c/databahn_export_ab_1_x.csv"]]}]}`))
	})
	exec.cfg.NamePrefix = "databahn_export_ab"

	result, err := exec.GetExecutionResult(context.Background(), "op-1")
	if err != nil {
		t.Fatalf("GetExecutionResult: %v", err)
	}
	if result.OutputLocation != "databahn_export_ab" {
		t.Fatalf(gotOutputLocation, result.OutputLocation)
	}
}

func TestADXGetExecutionResultToleratesDetailsFailure(t *testing.T) {
	exec, _ := newTestADXExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"details unavailable"}}`))
	})
	exec.cfg.NamePrefix = "databahn_export_ab"

	// Details are advisory — the staged blobs are still discoverable by prefix.
	result, err := exec.GetExecutionResult(context.Background(), "op-1")
	if err != nil {
		t.Fatalf("GetExecutionResult should tolerate a details failure: %v", err)
	}
	if result.OutputLocation != "databahn_export_ab" {
		t.Fatalf(gotOutputLocation, result.OutputLocation)
	}
}

func TestADXExecutorRequiresConnect(t *testing.T) {
	exec := NewADXExecutor(ADXConfig{ClusterURI: "https://cluster.kusto.windows.net", Database: "db"})
	if _, err := exec.CheckQueryStatus(context.Background(), "op"); err == nil {
		t.Fatal("expected error before Connect")
	}
}

func TestADXExecutorDefaults(t *testing.T) {
	exec := NewADXExecutor(ADXConfig{ClusterURI: "https://cluster.kusto.windows.net/", NamePrefix: "p"})
	if exec.cfg.ClusterURI != "https://cluster.kusto.windows.net" {
		t.Fatalf("cluster uri = %q (trailing slash must be trimmed for path joins)", exec.cfg.ClusterURI)
	}
	if exec.cfg.QueryTimeout != 30*time.Minute {
		t.Fatalf("query timeout = %v", exec.cfg.QueryTimeout)
	}
	if exec.Engine() != EngineADX {
		t.Fatalf("engine = %q", exec.Engine())
	}
	if exec.GetOutputLocation() != "p" {
		t.Fatalf("output location = %q", exec.GetOutputLocation())
	}
	if exec.GetAWSConfig() != nil {
		t.Fatal("ADX has no AWS config")
	}
	if _, err := exec.NewStagingReader("/tmp"); err == nil {
		t.Fatal("staging reader without blob config should error")
	}
}
