package query

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/store/destination"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

// Sentinel storage tiers, re-exported so the query package does not depend on callers
// importing the destination package for a string constant.
const (
	SentinelTierAnalytics = destination.SentinelTierAnalytics
	SentinelTierLake      = destination.SentinelTierLake
)

const defaultSentinelQueryTimeout = logAnalyticsMaxWaitSeconds * time.Second

// errSentinelRowBudget stops a stream once the export row budget is spent. It never escapes
// StreamRows.
var errSentinelRowBudget = errors.New("sentinel row budget exhausted")

// SentinelConfig holds everything needed to run KQL against a Microsoft Sentinel workspace.
type SentinelConfig struct {
	WorkspaceID string
	// WorkspaceName is required for the lake tier only: its KQL API addresses the workspace
	// as "workspaceName-workspaceId" rather than by GUID.
	WorkspaceName string
	StorageTier   string
	TenantID      string
	ClientID      string
	ClientSecret  string

	// Endpoint overrides the Azure public-cloud query host for sovereign-cloud deployments.
	// Empty in production. It must name an approved Log Analytics host: the Entra scope
	// follows it, so an arbitrary value would send a bearer token wherever it points.
	Endpoint string

	QueryTimeout time.Duration
	MaxRetries   int
}

// SentinelExecutor pulls rows from a Sentinel workspace over the query API and hands them to
// the pipeline's encoder.
//
// It implements RowStreamExecutor, not UnloadExecutor, because neither Sentinel tier accepts
// control commands: the Log Analytics query API has no `.export`, no server-side write
// target, and no operation id. So there is nothing to stage and nothing to reattach to, and
// a stale PROCESSING report restarts from scratch — the same behaviour Synapse's JDBC path
// already has.
type SentinelExecutor struct {
	cfg       SentinelConfig
	transport sentinelTransport
	columns   []string
	log       *zap.Logger
}

// NewSentinelExecutor selects the transport for the store's storage tier.
func NewSentinelExecutor(cfg SentinelConfig) (*SentinelExecutor, error) {
	if strings.TrimSpace(cfg.WorkspaceID) == "" {
		return nil, fmt.Errorf("workspace_id is required in connector configuration")
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = defaultSentinelQueryTimeout
	}
	log := logging.GetLogger()

	tier := destination.NormalizeSentinelTier(cfg.StorageTier)
	cfg.StorageTier = tier

	if err := validateSentinelEndpoint(tier, cfg.Endpoint); err != nil {
		return nil, err
	}

	e := &SentinelExecutor{cfg: cfg, log: log}
	switch tier {
	case SentinelTierAnalytics:
		e.transport = newLogAnalyticsTransport(cfg, log)
	case SentinelTierLake:
		database := (&destination.SentinelConfig{
			WorkspaceID:   cfg.WorkspaceID,
			WorkspaceName: cfg.WorkspaceName,
		}).LakeDatabase()
		if database == "" {
			return nil, fmt.Errorf("workspace_name is required in connector configuration when storage_tier is LAKE")
		}
		e.transport = newSentinelLakeTransport(cfg, database, log)
	default:
		return nil, fmt.Errorf("unsupported Sentinel storage_tier: %s", cfg.StorageTier)
	}
	return e, nil
}

func (e *SentinelExecutor) SetLogger(log *zap.Logger) {
	if log == nil {
		return
	}
	e.log = log
	switch t := e.transport.(type) {
	case *logAnalyticsTransport:
		t.log = log
	case *sentinelLakeTransport:
		t.log = log
	}
}

func (e *SentinelExecutor) Engine() string {
	if e.cfg.StorageTier == SentinelTierLake {
		return EngineSentinelLake
	}
	return EngineSentinelLAW
}

func (e *SentinelExecutor) Connect(ctx context.Context) error {
	if err := e.transport.Connect(ctx); err != nil {
		return err
	}
	e.log.Info("Connected to Microsoft Sentinel",
		zap.String("workspaceId", e.cfg.WorkspaceID),
		zap.String("storageTier", e.cfg.StorageTier))
	return nil
}

func (e *SentinelExecutor) Close() error { return nil }

// GetAWSConfig satisfies BaseExecutor; Sentinel has no AWS session.
func (e *SentinelExecutor) GetAWSConfig() interface{} { return nil }

// CancelQueryExecution satisfies RowStreamExecutor. Log Analytics runs the query inside the
// HTTP request, so cancelling the context is the only cancellation there is.
func (e *SentinelExecutor) CancelQueryExecution(ctx context.Context, executionID string) error {
	return nil
}

func (e *SentinelExecutor) ValidateExportQuery(ctx context.Context, kql string) error {
	if strings.TrimSpace(kql) == "" {
		return fmt.Errorf("Sentinel export query is required")
	}
	return nil
}

// GetQueryColumns returns the export's column names. The streaming path never needs this —
// columns arrive with the response schema — so this runs `| take 0` only when something asks
// out of band, and prefers the columns already seen.
func (e *SentinelExecutor) GetQueryColumns(ctx context.Context, kql, database string) ([]string, error) {
	if len(e.columns) > 0 {
		return e.columns, nil
	}
	var columns []string
	onColumns := func(cols []string) error {
		columns = cols
		return nil
	}
	if _, err := e.transport.Query(ctx, strings.TrimSpace(kql)+"\n| take 0", onColumns, func([]interface{}) error { return nil }); err != nil {
		return nil, err
	}
	return columns, nil
}

// StreamRows runs the export's query plan and emits every row through fn.
//
// opts.OnColumns fires once, before the first row, so the caller can build its encoder.
// opts.MaxRows is a defensive budget: backend-service already caps the query with a `take`,
// and this stops the stream if a response somehow exceeds it.
func (e *SentinelExecutor) StreamRows(ctx context.Context, kql string, opts StreamRowsOptions, fn func(row []interface{}) error) (int64, error) {
	plans := PlanSentinelQueries(kql)
	if len(plans) == 0 {
		return 0, fmt.Errorf("Sentinel query plan is empty")
	}

	var total int64
	started := time.Now()
	budget := opts.MaxRows

	onColumns := func(cols []string) error {
		if len(e.columns) == 0 {
			e.columns = cols
			e.log.Info("Sentinel export columns resolved",
				zap.Int("columnCount", len(cols)),
				zap.Strings("columns", cols))
			if opts.OnColumns != nil {
				return opts.OnColumns(cols)
			}
			return nil
		}
		// Reached only once chunking lands: every plan must agree on the schema, or rows
		// from different requests would be written under mismatched headers.
		if !sameColumns(e.columns, cols) {
			return fmt.Errorf("Sentinel query returned an inconsistent column set: %v then %v", e.columns, cols)
		}
		return nil
	}

	for _, plan := range plans {
		e.log.Info("Running Sentinel export query",
			zap.String("plan", plan.Label),
			zap.String("workspaceId", e.cfg.WorkspaceID),
			zap.Int64("rowsSoFar", total))

		onRow := func(row []interface{}) error {
			if budget > 0 && total >= budget {
				return errSentinelRowBudget
			}
			if err := fn(row); err != nil {
				return err
			}
			total++
			return nil
		}

		rows, err := e.transport.Query(ctx, plan.KQL, onColumns, onRow)
		if err != nil {
			if errors.Is(err, errSentinelRowBudget) {
				e.log.Warn("Sentinel export stopped at the row budget",
					zap.Int64("maxRows", budget),
					zap.Int64("totalRows", total))
				return total, nil
			}
			return total, fmt.Errorf("Sentinel export query (%s): %w", plan.Label, err)
		}

		e.log.Info("Sentinel export query complete",
			zap.String("plan", plan.Label),
			zap.Int64("rowsReturned", rows),
			zap.Int64("totalRows", total),
			zap.Duration("elapsed", time.Since(started)))
	}

	return total, nil
}

// logAnalyticsHosts are the Azure Monitor Logs query endpoints an export may target.
var logAnalyticsHosts = map[string]bool{
	"api.loganalytics.io":       true, // Azure public
	"api.loganalytics.us":       true, // US Gov
	"api.loganalytics.azure.cn": true, // China
}

// sentinelLakeHosts are the Sentinel data lake query endpoints an export may target.
var sentinelLakeHosts = map[string]bool{
	"api.securityplatform.microsoft.com": true,
}

// validateSentinelEndpoint picks the allowlist for the tier being queried. The two tiers use
// different services, so a lake endpoint is not valid for analytics and vice versa.
func validateSentinelEndpoint(tier, endpoint string) error {
	if tier == SentinelTierLake {
		return validateEndpointHost(endpoint, sentinelLakeHosts, "Sentinel data lake")
	}
	return validateLogAnalyticsEndpoint(endpoint)
}

// validateLogAnalyticsEndpoint constrains where the worker will send an Entra bearer token.
//
// The token is minted for the endpoint's own scope, so an unconstrained host would let a
// crafted configuration collect it. An empty endpoint means the Azure public default.
func validateLogAnalyticsEndpoint(endpoint string) error {
	return validateEndpointHost(endpoint, logAnalyticsHosts, "Azure Monitor Logs")
}

// validateEndpointHost accepts an empty endpoint, meaning the tier's public-cloud default.
func validateEndpointHost(endpoint string, allowed map[string]bool, service string) error {
	trimmed := strings.TrimSpace(endpoint)
	if trimmed == "" {
		return nil
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("sentinel endpoint %q is not a valid URL: %w", trimmed, err)
	}
	if !strings.EqualFold(parsed.Scheme, "https") {
		return fmt.Errorf("sentinel endpoint must use https, got %q", parsed.Scheme)
	}
	if parsed.User != nil {
		return fmt.Errorf("sentinel endpoint must not embed credentials")
	}
	if !allowed[strings.ToLower(parsed.Hostname())] {
		return fmt.Errorf("sentinel endpoint host %q is not a %s endpoint", parsed.Hostname(), service)
	}
	return nil
}

func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
