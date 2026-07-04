package pramaan

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

/*
PrometheusPramaan is a utility to start Prometheus and Pushgateway for module testing.

  - Pushgateway accepts pushed metrics via its HTTP API.
  - Prometheus scrapes the Pushgateway and exposes the full PromQL query API
    at /api/v1/query and /api/v1/query_range.

The service container receives urls.prometheus pointing to the Prometheus server
so it can execute PromQL queries from inside the Docker network.
*/
type PrometheusPramaan struct {
	t                    TestLogger
	PushgatewayContainer testcontainers.Container
	PrometheusContainer  testcontainers.Container
	pushgatewayExtURL    string
	prometheusExtURL     string
	prometheusIntURL     string
}

const (
	pushgatewayNetworkDns = "pramaanpushgateway"
	prometheusNetworkDns  = "pramaanprometheus"
	pushgatewayPort       = "9091"
	prometheusPort        = "9090"
)

// httpHostPortURL builds a base URL with scheme http://... suitable for http.Client.
// It uses net.JoinHostPort so IPv6 numeric hosts are bracketed correctly (e.g. [::1]:9091).
func httpHostPortURL(host string, port uint16) string {
	u := &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10)),
	}
	return u.String()
}

func NewPrometheusPramaan(ctx context.Context, t TestLogger, dockerNetwork *testcontainers.DockerNetwork) *PrometheusPramaan {
	// --- Pushgateway ---
	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "prom/pushgateway:v1.9.0",
			ExposedPorts: []string{pushgatewayPort + "/tcp"},
			WaitingFor:   wait.ForHTTP("/-/healthy").WithPort(pushgatewayPort + "/tcp").WithStartupTimeout(30 * time.Second),
			Networks:     []string{dockerNetwork.Name},
			NetworkAliases: map[string][]string{
				dockerNetwork.Name: {pushgatewayNetworkDns},
			},
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("Could not create pushgateway container: %v", err)
	}

	pgHost, err := pgContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get pushgateway host: %v", err)
	}
	pgPort, err := pgContainer.MappedPort(ctx, pushgatewayPort)
	if err != nil {
		log.Fatalf("failed to get pushgateway port: %v", err)
	}
	pushgatewayExtURL := httpHostPortURL(pgHost, pgPort.Num())

	// --- Prometheus (scrapes the Pushgateway) ---
	promConfig := fmt.Sprintf(`global:
  scrape_interval: 2s
  evaluation_interval: 2s
scrape_configs:
  - job_name: pushgateway
    honor_labels: true
    static_configs:
      - targets: ['%s:%s']
`, pushgatewayNetworkDns, pushgatewayPort)

	promContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "prom/prometheus:v2.53.0",
			ExposedPorts: []string{prometheusPort + "/tcp"},
			WaitingFor:   wait.ForHTTP("/-/healthy").WithPort(prometheusPort + "/tcp").WithStartupTimeout(30 * time.Second),
			Networks:     []string{dockerNetwork.Name},
			NetworkAliases: map[string][]string{
				dockerNetwork.Name: {prometheusNetworkDns},
			},
			Files: []testcontainers.ContainerFile{
				{
					Reader:            bytes.NewReader([]byte(promConfig)),
					ContainerFilePath: "/etc/prometheus/prometheus.yml",
					FileMode:          0o644,
				},
			},
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("Could not create prometheus container: %v", err)
	}

	promHost, err := promContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get prometheus host: %v", err)
	}
	promPort, err := promContainer.MappedPort(ctx, prometheusPort)
	if err != nil {
		log.Fatalf("failed to get prometheus port: %v", err)
	}

	prometheusExtURL := httpHostPortURL(promHost, promPort.Num())
	prometheusIntURL := fmt.Sprintf("http://%s:%s", prometheusNetworkDns, prometheusPort)

	return &PrometheusPramaan{
		t:                    t,
		PushgatewayContainer: pgContainer,
		PrometheusContainer:  promContainer,
		pushgatewayExtURL:    pushgatewayExtURL,
		prometheusExtURL:     prometheusExtURL,
		prometheusIntURL:     prometheusIntURL,
	}
}

// ---------------------------------------------------------------------------
// Push (via Pushgateway)
// ---------------------------------------------------------------------------

/*
PushMetric pushes a single gauge metric to the Pushgateway under the given job name.
labels is an optional map of additional grouping labels.

Example:

	prom.PushMetric("my_job", "http_requests_total", 42, map[string]string{"method": "GET"})
*/
func (p *PrometheusPramaan) PushMetric(job, metricName string, value float64, labels map[string]string) error {
	body := fmt.Sprintf("# TYPE %s gauge\n%s %g\n", metricName, metricName, value)
	return p.pushRaw(job, labels, body)
}

/*
PushCounterMetric pushes a counter metric to the Pushgateway under the given job name.
*/
func (p *PrometheusPramaan) PushCounterMetric(job, metricName string, value float64, labels map[string]string) error {
	body := fmt.Sprintf("# TYPE %s counter\n%s %g\n", metricName, metricName, value)
	return p.pushRaw(job, labels, body)
}

/*
PushRawMetrics pushes raw Prometheus exposition format text to the Pushgateway.
Use this when you need full control over the metric format.
*/
func (p *PrometheusPramaan) PushRawMetrics(job string, labels map[string]string, body string) error {
	return p.pushRaw(job, labels, body)
}

func (p *PrometheusPramaan) pushRaw(job string, labels map[string]string, body string) error {
	path := fmt.Sprintf("/metrics/job/%s", job)
	for k, v := range labels {
		path += fmt.Sprintf("/%s/%s", k, v)
	}

	reqURL := p.pushgatewayExtURL + path
	req, err := http.NewRequest(http.MethodPost, reqURL, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to push metric: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("pushgateway returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

/*
DeleteMetrics deletes all metrics for a given job (and optional grouping labels)
from the Pushgateway.
*/
func (p *PrometheusPramaan) DeleteMetrics(job string, labels map[string]string) error {
	path := fmt.Sprintf("/metrics/job/%s", job)
	for k, v := range labels {
		path += fmt.Sprintf("/%s/%s", k, v)
	}

	req, err := http.NewRequest(http.MethodDelete, p.pushgatewayExtURL+path, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("pushgateway delete returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Query (via Prometheus PromQL API)
// ---------------------------------------------------------------------------

/*
Query executes an instant PromQL query against Prometheus and returns the parsed
result. The returned QueryResult contains the result type and a slice of samples.

Example:

	result, err := prom.Query(`http_requests_total{method="GET"}`)
*/
func (p *PrometheusPramaan) Query(promQL string) (*QueryResult, error) {
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", p.prometheusExtURL, url.QueryEscape(promQL))

	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to query prometheus: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus returned status %d: %s", resp.StatusCode, string(body))
	}

	var apiResp prometheusAPIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse prometheus response: %w", err)
	}
	if apiResp.Status != "success" {
		return nil, fmt.Errorf("prometheus query failed: %s", apiResp.Error)
	}

	return parseQueryResult(apiResp.Data)
}

/*
QueryValue is a convenience method that executes a PromQL query and returns the
scalar value of the first result sample. Returns an error if there are no results.
*/
func (p *PrometheusPramaan) QueryValue(promQL string) (string, error) {
	result, err := p.Query(promQL)
	if err != nil {
		return "", err
	}
	if len(result.Samples) == 0 {
		return "", fmt.Errorf("query %q returned no results", promQL)
	}
	return result.Samples[0].Value, nil
}

// ---------------------------------------------------------------------------
// URL accessors
// ---------------------------------------------------------------------------

/*
GetPushgatewayExternalURL returns the host-accessible URL of the Pushgateway.
*/
func (p *PrometheusPramaan) GetPushgatewayExternalURL() string {
	return p.pushgatewayExtURL
}

/*
GetPrometheusExternalURL returns the host-accessible URL of Prometheus.
*/
func (p *PrometheusPramaan) GetPrometheusExternalURL() string {
	return p.prometheusExtURL
}

/*
GetPrometheusNetworkURL returns the Docker-internal URL of Prometheus,
used by containers on the same network.
*/
func (p *PrometheusPramaan) GetPrometheusNetworkURL() string {
	return p.prometheusIntURL
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

type prometheusAPIResponse struct {
	Status string          `json:"status"`
	Error  string          `json:"error,omitempty"`
	Data   json.RawMessage `json:"data"`
}

type prometheusDataEnvelope struct {
	ResultType string          `json:"resultType"`
	Result     json.RawMessage `json:"result"`
}

/*
QueryResult holds the parsed output of a PromQL instant query.
*/
type QueryResult struct {
	ResultType string
	Samples    []MetricSample
}

/*
MetricSample represents a single time series sample returned by a PromQL query.
*/
type MetricSample struct {
	Labels map[string]string
	Value  string
}

func parseQueryResult(raw json.RawMessage) (*QueryResult, error) {
	var envelope prometheusDataEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("failed to parse data envelope: %w", err)
	}

	qr := &QueryResult{ResultType: envelope.ResultType}

	switch envelope.ResultType {
	case "vector":
		var vectors []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]interface{}    `json:"value"`
		}
		if err := json.Unmarshal(envelope.Result, &vectors); err != nil {
			return nil, fmt.Errorf("failed to parse vector result: %w", err)
		}
		for _, v := range vectors {
			qr.Samples = append(qr.Samples, MetricSample{
				Labels: v.Metric,
				Value:  fmt.Sprintf("%v", v.Value[1]),
			})
		}
	case "scalar":
		var scalar [2]interface{}
		if err := json.Unmarshal(envelope.Result, &scalar); err != nil {
			return nil, fmt.Errorf("failed to parse scalar result: %w", err)
		}
		qr.Samples = append(qr.Samples, MetricSample{
			Value: fmt.Sprintf("%v", scalar[1]),
		})
	default:
		return nil, fmt.Errorf("unsupported result type: %s", envelope.ResultType)
	}

	return qr, nil
}
