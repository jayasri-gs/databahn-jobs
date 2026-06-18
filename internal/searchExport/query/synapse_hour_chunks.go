package query

import (
	"fmt"
	"time"
)

const hourMillis = 3_600_000

// HourChunk is one UTC hour window for Synapse export.
type HourChunk struct {
	StartMs int64
}

// PartitionColumns names hive partition fields exposed by the Synapse view.
type PartitionColumns struct {
	Year, Month, Day, Hour string
}

// DestinationPartitionColumns is used for DATABAHN_DESTINATION Azure Blob stores.
var DestinationPartitionColumns = PartitionColumns{
	Year:  "year_partition",
	Month: "month_partition",
	Day:   "day_partition",
	Hour:  "hour_partition",
}

// ExternalHivePartitionColumns is used for EXTERNAL_STORAGE Azure Blob datasets.
var ExternalHivePartitionColumns = PartitionColumns{
	Year:  "year",
	Month: "month",
	Day:   "date",
	Hour:  "hour",
}

// PartitionColumnsForStoreType picks partition column names from export config dataStoreType.
func PartitionColumnsForStoreType(dataStoreType string) PartitionColumns {
	if dataStoreType == "EXTERNAL_STORAGE" {
		return ExternalHivePartitionColumns
	}
	return DestinationPartitionColumns
}

// PlanHourChunks returns UTC hour-aligned chunks covering [startMs, endMs].
func PlanHourChunks(startMs, endMs int64) []HourChunk {
	if startMs <= 0 || endMs <= 0 || startMs > endMs {
		return nil
	}
	alignedStart := (startMs / hourMillis) * hourMillis
	alignedEnd := ((endMs + hourMillis - 1) / hourMillis) * hourMillis
	var chunks []HourChunk
	for t := alignedStart; t < alignedEnd; t += hourMillis {
		chunks = append(chunks, HourChunk{StartMs: t})
	}
	return chunks
}

// HourPartitionFilter builds a single-hour equality predicate for the given chunk start.
func HourPartitionFilter(startMs int64, cols PartitionColumns) string {
	t := time.UnixMilli(startMs).UTC()
	return fmt.Sprintf(
		"%s = '%04d' AND %s = '%02d' AND %s = '%02d' AND %s = '%02d'",
		cols.Year, t.Year(),
		cols.Month, int(t.Month()),
		cols.Day, t.Day(),
		cols.Hour, t.Hour(),
	)
}

// HourChunkFilter adds optional db_edge_ts bounds on the first/last hour of a range.
func HourChunkFilter(chunkStartMs, rangeStartMs, rangeEndMs int64, cols PartitionColumns) string {
	filter := HourPartitionFilter(chunkStartMs, cols)
	chunkEndMs := chunkStartMs + hourMillis
	if rangeStartMs > chunkStartMs {
		filter += fmt.Sprintf(" AND %s >= '%d'", synapseExportSortColumn, rangeStartMs)
	}
	if rangeEndMs < chunkEndMs-1 {
		filter += fmt.Sprintf(" AND %s <= '%d'", synapseExportSortColumn, rangeEndMs)
	}
	return filter
}
