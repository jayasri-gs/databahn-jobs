package query

import (
	"os"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/utils"
)

const (
	defaultMaxExportRows          = 10_000_000
	defaultSynapseQueryTimeoutMin = 120
	maxLOBBytes                   = 1 << 20
	synapseProgressInterval       = 30 * time.Second
	synapsePreflightTimeout       = 5 * time.Minute
)

type StreamRowsOptions struct {
	MaxRows           int64
	ServerRowCap      int64 // when set, Synapse runs SET ROWCOUNT before the export query
	QueryTimeout      time.Duration
	ProgressEvery     time.Duration
	ProgressEveryRows int64
	SkipPreflight     bool
	OnColumns         func([]string) error
	HourBatchRows     int64
}

func StreamRowsOptionsFromEnv() StreamRowsOptions {
	progressEveryRows := int64(utils.GetEnvInt("SEARCH_EXPORT_SYNAPSE_PROGRESS_EVERY_ROWS", 0))

	maxRows := int64(defaultMaxExportRows)
	var serverRowCap int64
	if _, ok := os.LookupEnv("SEARCH_EXPORT_MAX_ROWS"); ok {
		maxRows = int64(utils.GetEnvInt("SEARCH_EXPORT_MAX_ROWS", defaultMaxExportRows))
		if maxRows > 0 {
			serverRowCap = maxRows
		}
	}

	skipPreflight := utils.GetEnvBool("SEARCH_EXPORT_SYNAPSE_SKIP_PREFLIGHT", true)
	if serverRowCap > 0 {
		skipPreflight = true
	}

	return StreamRowsOptions{
		MaxRows:           maxRows,
		ServerRowCap:      serverRowCap,
		QueryTimeout:      time.Duration(utils.GetEnvInt("SEARCH_EXPORT_SYNAPSE_QUERY_TIMEOUT_MINUTES", defaultSynapseQueryTimeoutMin)) * time.Minute,
		ProgressEvery:     synapseProgressInterval,
		ProgressEveryRows: progressEveryRows,
		SkipPreflight:     skipPreflight,
		HourBatchRows:     int64(utils.GetEnvInt("SEARCH_EXPORT_SYNAPSE_HOUR_BATCH_ROWS", 10_000)),
	}
}

// Sentinel export defaults. The query timeout matches the Log Analytics service ceiling —
// there is no way to ask for longer than ten minutes.
const (
	defaultSentinelQueryTimeoutSeconds = 600
	defaultSentinelMaxRetries          = 3
	defaultSentinelMaxRows             = 500_000

	// maxSentinelRetries bounds the retry budget. Each throttled attempt waits up to 60s, so
	// an unbounded count read from the environment would park the job indefinitely.
	maxSentinelRetries = 10

	// sentinelLakeMaxTimeout matches the servertimeout the lake query API is given.
	sentinelLakeMaxTimeout = 4 * time.Minute
)

// SentinelStreamOptionsFromEnv builds the row-stream options for a Sentinel export.
//
// MaxRows is a defensive backstop, not the real limit: backend-service caps the query with a
// `take` sized to keep one response inside the service's 500k-row / ~64 MB budget. This only
// stops a stream that somehow exceeds it.
func SentinelStreamOptionsFromEnv() StreamRowsOptions {
	return StreamRowsOptions{
		MaxRows:       int64(utils.GetEnvInt("SEARCH_EXPORT_SENTINEL_MAX_ROWS", defaultSentinelMaxRows)),
		QueryTimeout:  sentinelQueryTimeout(),
		ProgressEvery: synapseProgressInterval,
		SkipPreflight: true,
	}
}

// sentinelQueryTimeout clamps the configured timeout to the Log Analytics service ceiling.
// Waiting longer than the service will ever take just holds the request context open.
func sentinelQueryTimeout() time.Duration {
	seconds := utils.GetEnvInt("SEARCH_EXPORT_SENTINEL_QUERY_TIMEOUT_SECONDS", defaultSentinelQueryTimeoutSeconds)
	if seconds < 1 {
		seconds = defaultSentinelQueryTimeoutSeconds
	}
	if seconds > defaultSentinelQueryTimeoutSeconds {
		seconds = defaultSentinelQueryTimeoutSeconds
	}
	return time.Duration(seconds) * time.Second
}

// SentinelStreamOptionsForTier picks the per-tier limits. The lake API takes a 4-minute
// servertimeout rather than the analytics tier's 10-minute ceiling, so waiting longer than
// that just holds the request context open.
func SentinelStreamOptionsForTier(tier string) StreamRowsOptions {
	opts := SentinelStreamOptionsFromEnv()
	if strings.EqualFold(tier, "LAKE") || tier == "KUSTO_LAKE" {
		if opts.QueryTimeout > sentinelLakeMaxTimeout {
			opts.QueryTimeout = sentinelLakeMaxTimeout
		}
	}
	return opts
}

// SentinelMaxRetriesFromEnv is the retry budget for throttled Log Analytics requests,
// clamped so a misconfigured environment cannot stall the job.
func SentinelMaxRetriesFromEnv() int {
	retries := utils.GetEnvInt("SEARCH_EXPORT_SENTINEL_MAX_RETRIES", defaultSentinelMaxRetries)
	if retries < 0 {
		return 0
	}
	if retries > maxSentinelRetries {
		return maxSentinelRetries
	}
	return retries
}
