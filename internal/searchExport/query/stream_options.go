package query

import (
	"os"
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
