package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"go.uber.org/zap"
)

func (p *Pipeline) buildExportObjectKey(ext string) (fileName, outputKey string) {
	fileName = exportFileName(p.exportName, p.request.TableName, p.request.DataSetName, ext, p.reportID)
	outputKey = fmt.Sprintf("exports/%s/%s", p.reportID, fileName)
	return fileName, outputKey
}

func (p *Pipeline) runSynapseStreamExport(ctx context.Context, destBucket string, syn query.RowStreamExecutor) (*PipelineResult, error) {
	p.log.Info("Running Synapse JDBC stream export")

	opts := query.StreamRowsOptionsFromEnv()
	var columns []string
	sortColIdx := -1

	if opts.SkipPreflight {
		opts.OnColumns = func(cols []string) error {
			columns = cols
			sortColIdx = columnIndex(cols, "db_edge_ts")
			p.log.Info("Synapse stream columns discovered",
				zap.Int("columnCount", len(cols)),
				zap.Strings("columns", cols))
			return nil
		}
	} else if err := syn.ValidateExportQuery(ctx, p.request.Query); err != nil {
		return nil, err
	} else {
		var err error
		columns, err = syn.GetQueryColumns(ctx, p.request.Query, p.request.Database)
		if err != nil {
			return nil, err
		}
		sortColIdx = columnIndex(columns, "db_edge_ts")
	}

	exportFormat := normalizedFormat(p.request.Format)
	ext, contentType := formatMeta(exportFormat)
	fileName, outputKey := p.buildExportObjectKey(ext)
	p.log.Info("Export output file", zap.String("fileName", fileName), zap.String("outputKey", outputKey))

	p.abortOrphanedMultipartUploads(ctx, destBucket)
	if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
		return nil, fmt.Errorf("failed to init uploader: %w", err)
	}

	partCols := query.PartitionColumnsForStoreType(p.request.DataStoreType)
	rangeStartMs := p.request.StartTime
	rangeEndMs := p.request.EndTime
	hours := query.PlanHourChunks(rangeStartMs, rangeEndMs)
	useHourChunks := len(hours) > 0
	if useHourChunks && opts.HourBatchRows > 0 && sortColIdx < 0 && !opts.SkipPreflight {
		return nil, fmt.Errorf("hour-chunk export requires db_edge_ts column in query result")
	}

	if useHourChunks {
		p.log.Info("Synapse export hour plan",
			zap.Int("totalHours", len(hours)),
			zap.Int64("rangeStartMs", rangeStartMs),
			zap.Int64("rangeEndMs", rangeEndMs),
			zap.Time("rangeStartUTC", time.UnixMilli(rangeStartMs).UTC()),
			zap.Time("rangeEndUTC", time.UnixMilli(rangeEndMs).UTC()))
	}

	remaining := opts.MaxRows

	totalRows, totalBytes, err := p.encodeRowsToUploader(ctx, func() []string { return columns }, func(cb func([]interface{}) error) error {
		if !useHourChunks {
			streamOpts := synapseStreamOpts(opts, remaining)
			n, streamErr := syn.StreamRows(ctx, p.request.Query, streamOpts, cb)
			if streamErr != nil {
				return streamErr
			}
			if remaining > 0 {
				remaining -= n
			}
			return nil
		}

		for hi := 0; hi < len(hours); hi++ {
			hourStartMs := hours[hi].StartMs
			hourStartUTC := time.UnixMilli(hourStartMs).UTC()
			hourFilter := query.HourChunkFilter(hourStartMs, rangeStartMs, rangeEndMs, partCols)
			hourSQL := query.AddPartitionFilter(p.request.Query, hourFilter)
			p.log.Info("Synapse export hour started",
				zap.Int("hourIndex", hi+1),
				zap.Int("totalHours", len(hours)),
				zap.Time("hourStartUTC", hourStartUTC),
				zap.String("hourFilter", hourFilter))
			key := ""
			for sub := 0; ; sub++ {
				chunkSQL := query.BuildSubChunkQuery(hourSQL, opts.HourBatchRows, key)
				p.log.Debug("Synapse export chunk query SQL", zap.String("query", chunkSQL))
				p.log.Info("Synapse export chunk query",
					zap.Int("hourIndex", hi+1),
					zap.Int("totalHours", len(hours)),
					zap.Time("hourStartUTC", hourStartUTC),
					zap.Int("subChunk", sub+1),
					zap.String("lastSortKey", key),
					zap.Int64("batchLimit", opts.HourBatchRows),
					zap.String("queryHash", sqlFingerprint(chunkSQL)))

				streamOpts := synapseStreamOpts(opts, remaining)
				var batchMaxKey string
				wrap := func(row []interface{}) error {
					if k := cellSortKey(row, sortColIdx); k != "" {
						batchMaxKey = k
					}
					return cb(row)
				}
				n, streamErr := syn.StreamRows(ctx, chunkSQL, streamOpts, wrap)
				if streamErr != nil {
					return streamErr
				}
				if remaining > 0 {
					remaining -= n
					if remaining <= 0 {
						p.log.Info("Synapse export chunk complete",
							zap.Int("hourIndex", hi+1),
							zap.Int("totalHours", len(hours)),
							zap.Time("hourStartUTC", hourStartUTC),
							zap.Int("subChunk", sub+1),
							zap.Int64("rowsInChunk", n),
							zap.String("batchMaxKey", batchMaxKey),
							zap.Int64("remainingRowBudget", remaining))
						return nil
					}
				}
				p.log.Info("Synapse export chunk complete",
					zap.Int("hourIndex", hi+1),
					zap.Int("totalHours", len(hours)),
					zap.Time("hourStartUTC", hourStartUTC),
					zap.Int("subChunk", sub+1),
					zap.Int64("rowsInChunk", n),
					zap.String("batchMaxKey", batchMaxKey),
					zap.Int64("remainingRowBudget", remaining))
				if batchMaxKey != "" {
					key = batchMaxKey
				}
				if opts.HourBatchRows > 0 && n >= opts.HourBatchRows && batchMaxKey == "" {
					return fmt.Errorf("hour-chunk pagination requires db_edge_ts column with non-null values in result set")
				}
				if n == 0 || opts.HourBatchRows <= 0 || n < opts.HourBatchRows {
					break
				}
			}
			p.log.Info("Synapse export hour complete",
				zap.Int("hourIndex", hi+1),
				zap.Int("totalHours", len(hours)),
				zap.Time("hourStartUTC", hourStartUTC))
		}
		return nil
	})
	if err != nil {
		_ = syn.CancelQueryExecution(ctx, "")
		_ = p.uploader.Abort(ctx)
		return nil, fmt.Errorf("synapse stream export: %w", err)
	}

	if totalBytes == 0 {
		return &PipelineResult{TotalRows: totalRows, TotalBytes: 0}, nil
	}

	presignedURL, err := p.uploader.GeneratePresignedURL(ctx, p.config.PresignExpiry)
	if err != nil {
		p.log.Warn("Failed to generate presigned URL", zap.Error(err))
	}

	return &PipelineResult{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		Location:     p.uploader.GetLocation(),
		PresignedURL: presignedURL,
		Expiry:       time.Now().Add(p.config.PresignExpiry),
	}, nil
}

func synapseStreamOpts(opts query.StreamRowsOptions, remaining int64) query.StreamRowsOptions {
	streamOpts := opts
	if remaining > 0 {
		streamOpts.MaxRows = remaining
		if streamOpts.ServerRowCap > 0 {
			if remaining < streamOpts.ServerRowCap {
				streamOpts.ServerRowCap = remaining
			}
		}
	}
	return streamOpts
}

func columnIndex(columns []string, name string) int {
	want := strings.ToLower(name)
	for i, c := range columns {
		if strings.ToLower(c) == want {
			return i
		}
	}
	return -1
}

func cellSortKey(row []interface{}, idx int) string {
	if idx < 0 || idx >= len(row) || row[idx] == nil {
		return ""
	}
	switch v := row[idx].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func sqlFingerprint(sql string) string {
	sum := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(sum[:8])
}
