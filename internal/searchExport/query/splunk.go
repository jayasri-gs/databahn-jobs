package query

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const (
	defaultSplunkPort         = 8089
	defaultSplunkQueryTimeout = 30 * time.Minute
	// defaultSplunkMaxResultRows mirrors Splunk [searchresults] maxresultrows unless the
	// deployment overrides it — aggregation exports hitting this count exactly are treated
	// as truncated.
	defaultSplunkMaxResultRows = 50_000
)

var (
	splunkAggregationCommand = regexp.MustCompile(`(?i)(?:^|[|\n])\s*(stats|chart|timechart)\b`)
	splunkHeadLimitPattern   = regexp.MustCompile(`(?i)\|\s*head\s+(\d+)\b`)
)

// errSplunkRowBudget stops a stream once the export row budget is spent. It never escapes
// StreamRows.
var errSplunkRowBudget = errors.New("splunk row budget exhausted")

// SplunkConfig holds everything needed to run SPL against a Splunk search head.
type SplunkConfig struct {
	Host              string
	Port              int
	Scheme            string
	Token             string
	SSLCertValidation bool
	Index             string

	QueryTimeout time.Duration
	MaxRetries   int
}

// SplunkExecutor pulls rows from a Splunk search head over the jobs/export API and hands
// them to the pipeline's encoder.
//
// It implements RowStreamExecutor, not UnloadExecutor, because Splunk has no server-side
// write target: results must be pulled over HTTP and encoded in the worker. There is nothing
// to stage and nothing to reattach to, and a stale PROCESSING report restarts from scratch —
// the same behaviour Sentinel's query-API path already has.
type SplunkExecutor struct {
	cfg       SplunkConfig
	transport splunkTransport
	columns   []string
	log       *zap.Logger
}

// splunkTransport is the HTTP seam for Splunk export. The implementation lives in
// splunk_transport.go.
type splunkTransport interface {
	Connect(ctx context.Context) error
	Export(ctx context.Context, spl string, onColumns func([]string) error, onRow func([]interface{}) error) (int64, SplunkStreamMetadata, error)
}

// NewSplunkExecutor validates configuration and wires the export transport.
func NewSplunkExecutor(cfg SplunkConfig) (*SplunkExecutor, error) {
	normalized, err := normalizeSplunkConfig(cfg)
	if err != nil {
		return nil, err
	}
	log := logging.GetLogger()
	transport, err := newSplunkTransport(normalized, log)
	if err != nil {
		return nil, err
	}
	return &SplunkExecutor{
		cfg:       normalized,
		transport: transport,
		log:       log,
	}, nil
}

func normalizeSplunkConfig(cfg SplunkConfig) (SplunkConfig, error) {
	cfg.Host = strings.TrimSpace(cfg.Host)
	cfg.Scheme = strings.ToLower(strings.TrimSpace(cfg.Scheme))
	cfg.Token = strings.TrimSpace(cfg.Token)
	cfg.Index = strings.TrimSpace(cfg.Index)

	if cfg.Host == "" {
		return SplunkConfig{}, fmt.Errorf("splunk_host is required in connector configuration")
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "https"
	}
	switch cfg.Scheme {
	case "http", "https":
	default:
		return SplunkConfig{}, fmt.Errorf("splunk scheme must be http or https, got %q", cfg.Scheme)
	}
	if cfg.Port <= 0 {
		cfg.Port = defaultSplunkPort
	}
	if cfg.Token == "" {
		return SplunkConfig{}, fmt.Errorf("splunk search token is required")
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = defaultSplunkQueryTimeout
	}
	return cfg, nil
}

func (e *SplunkExecutor) SetLogger(log *zap.Logger) {
	if log == nil {
		return
	}
	e.log = log
	if t, ok := e.transport.(*splunkHTTPTransport); ok {
		t.log = log
	}
}

func (e *SplunkExecutor) Engine() string { return EngineSplunk }

func (e *SplunkExecutor) Connect(ctx context.Context) error {
	if err := e.transport.Connect(ctx); err != nil {
		return err
	}
	e.log.Info("Connected to Splunk",
		zap.String("host", e.cfg.Host),
		zap.Int("port", e.cfg.Port),
		zap.String("scheme", e.cfg.Scheme))
	return nil
}

func (e *SplunkExecutor) Close() error { return nil }

// GetAWSConfig satisfies BaseExecutor; Splunk has no AWS session.
func (e *SplunkExecutor) GetAWSConfig() interface{} { return nil }

// CancelQueryExecution satisfies RowStreamExecutor. The export endpoint runs the search
// inside the HTTP request, so cancelling the context is the only cancellation there is.
func (e *SplunkExecutor) CancelQueryExecution(ctx context.Context, executionID string) error {
	return nil
}

func (e *SplunkExecutor) ValidateExportQuery(ctx context.Context, spl string) error {
	if strings.TrimSpace(spl) == "" {
		return fmt.Errorf("Splunk export query is required")
	}
	return nil
}

// GetQueryColumns returns the export's column names. The streaming path never needs this —
// columns arrive with the json_rows schema — so this runs a zero-row probe only when
// something asks out of band, and prefers the columns already seen.
func (e *SplunkExecutor) GetQueryColumns(ctx context.Context, spl, database string) ([]string, error) {
	if len(e.columns) > 0 {
		return e.columns, nil
	}
	var columns []string
	onColumns := func(cols []string) error {
		columns = cols
		return nil
	}
	probe := strings.TrimSpace(spl) + " | head 0"
	_, _, err := e.transport.Export(ctx, probe, onColumns, func([]interface{}) error { return nil })
	if err != nil {
		return nil, err
	}
	return columns, nil
}

// SplunkQueryPlan is one export request: the SPL to run verbatim, plus a label for logs.
type SplunkQueryPlan struct {
	SPL   string
	Label string
}

// PlanSplunkQueries returns the sequence of Splunk export requests that together satisfy
// one export. Today it always returns a single request running the planned SPL verbatim.
func PlanSplunkQueries(spl string) []SplunkQueryPlan {
	return []SplunkQueryPlan{{SPL: strings.TrimSpace(spl), Label: "full-range"}}
}

// StreamRows runs the export's query plan and emits every row through fn.
//
// opts.OnColumns fires once, before the first row, so the caller can build its encoder.
// opts.MaxRows is a defensive budget: backend-service already caps the query with a head,
// and this stops the stream if a response somehow exceeds it.
func (e *SplunkExecutor) StreamRows(ctx context.Context, spl string, opts StreamRowsOptions, fn func(row []interface{}) error) (int64, error) {
	plans := PlanSplunkQueries(spl)
	if len(plans) == 0 {
		return 0, fmt.Errorf("Splunk query plan is empty")
	}

	var total int64
	var streamMeta SplunkStreamMetadata
	started := time.Now()
	budget := opts.MaxRows

	onColumns := func(cols []string) error {
		if len(e.columns) == 0 {
			e.columns = cols
			e.log.Info("Splunk export columns resolved",
				zap.Int("columnCount", len(cols)),
				zap.Strings("columns", cols))
			if opts.OnColumns != nil {
				return opts.OnColumns(cols)
			}
			return nil
		}
		if !sameSplunkColumns(e.columns, cols) {
			return fmt.Errorf("Splunk query returned an inconsistent column set: %v then %v", e.columns, cols)
		}
		return nil
	}

	for _, plan := range plans {
		e.log.Info("Running Splunk export query",
			zap.String("plan", plan.Label),
			zap.String("host", e.cfg.Host),
			zap.Int64("rowsSoFar", total))

		onRow := func(row []interface{}) error {
			if budget > 0 && total >= budget {
				return errSplunkRowBudget
			}
			if err := fn(row); err != nil {
				return err
			}
			total++
			return nil
		}

		rows, meta, err := e.transport.Export(ctx, plan.SPL, onColumns, onRow)
		streamMeta = mergeSplunkStreamMetadata(streamMeta, meta)
		if err != nil {
			if errors.Is(err, errSplunkRowBudget) {
				e.log.Warn("Splunk export stopped at the row budget",
					zap.Int64("maxRows", budget),
					zap.Int64("totalRows", total))
				return total, nil
			}
			return total, fmt.Errorf("Splunk export query (%s): %w", plan.Label, err)
		}

		if rows != total {
			e.log.Warn("Splunk export row count mismatch between decoder and callback",
				zap.Int64("decoderRows", rows),
				zap.Int64("streamedRows", total))
		}

		if err := reconcileSplunkExportTruncation(plan.SPL, total, streamMeta); err != nil {
			return total, fmt.Errorf("Splunk export query (%s): %w", plan.Label, err)
		}

		e.log.Info("Splunk export query complete",
			zap.String("plan", plan.Label),
			zap.Int64("rowsReturned", rows),
			zap.Int64("totalRows", total),
			zap.Duration("elapsed", time.Since(started)))
	}

	return total, nil
}

func mergeSplunkStreamMetadata(base, add SplunkStreamMetadata) SplunkStreamMetadata {
	base.Messages = append(base.Messages, add.Messages...)
	base.DroppedFieldNames = appendUniqueStrings(base.DroppedFieldNames, add.DroppedFieldNames...)
	base.TruncationHints = appendUniqueStrings(base.TruncationHints, add.TruncationHints...)
	base.PreviewFramesSkipped += add.PreviewFramesSkipped
	return base
}

func appendUniqueStrings(base []string, extra ...string) []string {
	for _, value := range extra {
		base = appendUniqueString(base, value)
	}
	return base
}

func splunkMaxResultRowsLimit() int64 {
	limit := int64(utils.GetEnvInt("SEARCH_EXPORT_SPLUNK_MAX_RESULT_ROWS", defaultSplunkMaxResultRows))
	if limit <= 0 {
		return defaultSplunkMaxResultRows
	}
	return limit
}

// isSplunkAggregationPipeline reports whether the planned SPL ends in a transforming command
// whose output is capped by Splunk [searchresults] maxresultrows.
func isSplunkAggregationPipeline(spl string) bool {
	return splunkAggregationCommand.FindStringIndex(spl) != nil
}

// parseSplunkHeadLimit returns the last explicit | head N limit in the planned SPL.
func parseSplunkHeadLimit(spl string) (int64, bool) {
	matches := splunkHeadLimitPattern.FindAllStringSubmatch(spl, -1)
	if len(matches) == 0 {
		return 0, false
	}
	last := matches[len(matches)-1]
	if len(last) < 2 {
		return 0, false
	}
	limit, err := strconv.ParseInt(last[1], 10, 64)
	if err != nil || limit < 0 {
		return 0, false
	}
	return limit, true
}

func reconcileSplunkExportTruncation(spl string, rows int64, meta SplunkStreamMetadata) error {
	if rows == 0 {
		return nil
	}

	if msg := splunkExplicitTruncationMessage(meta); msg != "" {
		return fmt.Errorf("Splunk export appears truncated: %s", msg)
	}

	limit := splunkMaxResultRowsLimit()
	if isSplunkAggregationPipeline(spl) {
		if rows == limit {
			return fmt.Errorf(
				"Splunk aggregation export returned exactly %d rows, which matches the Splunk maxresultrows limit — the result was likely truncated. Narrow the query or reduce aggregation groups",
				limit)
		}
		return nil
	}

	if headLimit, ok := parseSplunkHeadLimit(spl); ok && rows == headLimit {
		return nil
	}

	return nil
}

func splunkExplicitTruncationMessage(meta SplunkStreamMetadata) string {
	if len(meta.TruncationHints) > 0 {
		return meta.TruncationHints[0]
	}
	for _, msg := range meta.Messages {
		if splunkMessageIndicatesTruncation(msg) {
			return boundSplunkText(msg.Text)
		}
	}
	return ""
}

func splunkMessageIndicatesTruncation(msg SplunkResponseMessage) bool {
	text := strings.ToLower(strings.TrimSpace(msg.Text))
	if text == "" {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(msg.Type)) {
	case "ERROR", "FATAL", "WARN", "WARNING":
	default:
		if !strings.Contains(text, "truncat") && !strings.Contains(text, "maxresultrows") {
			return false
		}
	}
	return strings.Contains(text, "truncat") ||
		strings.Contains(text, "maxresultrows") ||
		strings.Contains(text, "limit")
}

func sameSplunkColumns(a, b []string) bool {
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

var _ RowStreamExecutor = (*SplunkExecutor)(nil)
