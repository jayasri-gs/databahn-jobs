package query

import (
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
	MaxRows       int64
	QueryTimeout  time.Duration
	ProgressEvery time.Duration
}

func StreamRowsOptionsFromEnv() StreamRowsOptions {
	return StreamRowsOptions{
		MaxRows:       int64(utils.GetEnvInt("SEARCH_EXPORT_MAX_ROWS", defaultMaxExportRows)),
		QueryTimeout:  time.Duration(utils.GetEnvInt("SEARCH_EXPORT_SYNAPSE_QUERY_TIMEOUT_MINUTES", defaultSynapseQueryTimeoutMin)) * time.Minute,
		ProgressEvery: synapseProgressInterval,
	}
}
