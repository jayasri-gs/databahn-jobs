package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/state"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/upload"
	"go.uber.org/zap"
)

func (p *Pipeline) buildExportObjectKey(ext string) (fileName, outputKey string) {
	fileName = exportFileName(p.exportName, p.request.TableName, p.request.DataSetName, ext, p.reportID)
	outputKey = fmt.Sprintf("exports/%s/%s", p.reportID, fileName)
	return fileName, outputKey
}

func (p *Pipeline) runSynapseStreamExport(ctx context.Context, destBucket string, syn query.RowStreamExecutor) (*PipelineResult, error) {
	var resumeCP *state.Checkpoint
	if p.config.EFSMountPath != "" {
		cp, err := state.Read(p.config.EFSMountPath, p.reportID)
		if err == nil && cp != nil && cp.Stage == state.StageStreaming {
			resumeCP = cp
		}
	}
	return p.runSynapseStreamExportWithCheckpoint(ctx, destBucket, syn, resumeCP)
}

func (p *Pipeline) runSynapseStreamExportWithCheckpoint(ctx context.Context, destBucket string, syn query.RowStreamExecutor, resumeCP *state.Checkpoint) (*PipelineResult, error) {
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

	var encodeResume *EncodeResume
	if resumeCP != nil && resumeCP.UploadID != "" && resumeCP.Key == outputKey {
		if err := p.reattachSynapseUpload(resumeCP); err != nil {
			p.log.Warn("Failed to reattach export upload; starting fresh", zap.Error(err))
			p.abortCheckpointedUpload(ctx, resumeCP)
			resumeCP = nil
		} else {
			encodeResume = &EncodeResume{
				StartPartNumber: resumeCP.LastUploadedPart + 1,
				RowsProcessed:   resumeCP.RowsProcessed,
				BytesProcessed:  resumeCP.BytesProcessed,
			}
			encodeResume.ExistingParts = p.committedUploadParts(ctx, resumeCP)
		}
	}

	if p.uploader.UploadID() == "" {
		if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
			return nil, fmt.Errorf("failed to init uploader: %w", err)
		}
	}

	partCols := query.PartitionColumnsForStoreType(p.request.DataStoreType)
	rangeStartMs := p.request.StartTime
	rangeEndMs := p.request.EndTime
	hours := query.PlanHourChunks(rangeStartMs, rangeEndMs)
	useHourChunks := len(hours) > 0

	startHour := 0
	subStart := 0
	lastSortKey := ""
	if resumeCP != nil && useHourChunks {
		startHour = resumeCP.SynapseHourIndex
		subStart = resumeCP.SynapseSubChunkIndex
		lastSortKey = resumeCP.SynapseLastSortKey
	}

	progress := &synapseChunkProgress{
		hourIndex: startHour,
		subIndex:  subStart,
		sortKey:   lastSortKey,
	}
	if resumeCP != nil {
		progress.lastPartNum = resumeCP.LastUploadedPart
		progress.rowsProcessed = resumeCP.RowsProcessed
		progress.bytesProcessed = resumeCP.BytesProcessed
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

		for hi := progress.hourIndex; hi < len(hours); hi++ {
			hourFilter := query.HourChunkFilter(hours[hi].StartMs, rangeStartMs, rangeEndMs, partCols)
			hourSQL := query.AddPartitionFilter(p.request.Query, hourFilter)
			sub := 0
			key := ""
			if hi == progress.hourIndex {
				sub = progress.subIndex
				key = progress.sortKey
			}
			for ; ; sub++ {
				chunkSQL := query.BuildSubChunkQuery(hourSQL, opts.HourBatchRows, key)
				p.log.Info("Synapse export chunk",
					zap.Int("hour", hi+1),
					zap.Int("totalHours", len(hours)),
					zap.Int("subChunk", sub+1))

				streamOpts := synapseStreamOpts(opts, remaining)
				var batchMaxKey string
				wrap := func(row []interface{}) error {
					if k := cellSortKey(row, sortColIdx); k != "" {
						batchMaxKey = k
						progress.pendingMaxKey = k
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
						progress.hourIndex = hi
						progress.subIndex = sub + 1
						if progress.pendingMaxKey != "" {
							progress.sortKey = progress.pendingMaxKey
						} else if batchMaxKey != "" {
							progress.sortKey = batchMaxKey
						}
						return nil
					}
				}
				if batchMaxKey != "" {
					key = batchMaxKey
				}
				progress.hourIndex = hi
				progress.subIndex = sub + 1
				if err := p.writeSynapseStreamCheckpointFromProgress(destBucket, outputKey, len(hours), progress); err != nil {
					return err
				}
				if n == 0 || opts.HourBatchRows <= 0 || n < opts.HourBatchRows {
					break
				}
			}
			progress.hourIndex = hi + 1
			progress.subIndex = 0
			progress.sortKey = ""
			progress.pendingMaxKey = ""
			if err := p.writeSynapseStreamCheckpointFromProgress(destBucket, outputKey, len(hours), progress); err != nil {
				return err
			}
		}
		return nil
	}, func(partNum int, rows, bytes int64) error {
		progress.lastPartNum = partNum
		progress.rowsProcessed = rows
		progress.bytesProcessed = bytes
		if progress.pendingMaxKey != "" {
			progress.sortKey = progress.pendingMaxKey
		}
		return p.writeSynapseStreamCheckpointFromProgress(destBucket, outputKey, len(hours), progress)
	}, encodeResume)
	if err != nil {
		_ = syn.CancelQueryExecution(ctx, "")
		_ = p.uploader.Abort(ctx)
		p.cleanupCheckpoint()
		return nil, fmt.Errorf("synapse stream export: %w", err)
	}

	if totalBytes == 0 {
		p.cleanupCheckpoint()
		return &PipelineResult{TotalRows: totalRows, TotalBytes: 0}, nil
	}

	presignedURL, err := p.uploader.GeneratePresignedURL(ctx, p.config.PresignExpiry)
	if err != nil {
		p.log.Warn("Failed to generate presigned URL", zap.Error(err))
	}

	p.cleanupCheckpoint()

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

func (p *Pipeline) committedUploadParts(ctx context.Context, cp *state.Checkpoint) []upload.PartInfo {
	if cp == nil || cp.LastUploadedPart <= 0 {
		return nil
	}
	if s3up, ok := p.uploader.(*upload.S3Uploader); ok {
		existing, listErr := s3up.ListParts(ctx)
		if listErr != nil {
			return nil
		}
		committed := make([]upload.PartInfo, 0, cp.LastUploadedPart)
		for _, part := range existing {
			if part.PartNumber <= cp.LastUploadedPart {
				committed = append(committed, part)
			}
		}
		return committed
	}
	if len(cp.UploadBlockIDs) > 0 {
		return upload.PartInfosThrough(len(cp.UploadBlockIDs))
	}
	return upload.PartInfosThrough(cp.LastUploadedPart)
}

func (p *Pipeline) reattachSynapseUpload(cp *state.Checkpoint) error {
	if cp == nil || cp.UploadID == "" {
		return fmt.Errorf("checkpoint missing upload id")
	}
	if s3up, ok := p.uploader.(*upload.S3Uploader); ok {
		s3up.ReattachMultipart(cp.Bucket, cp.Key, cp.UploadID)
		return nil
	}
	if azureUp, ok := p.uploader.(*upload.AzureUploader); ok {
		blockIDs := cp.UploadBlockIDs
		if len(blockIDs) == 0 {
			blockIDs = upload.BlockIDsThrough(cp.LastUploadedPart)
		}
		return azureUp.ReattachMultipart(cp.Bucket, cp.Key, cp.UploadID, blockIDs)
	}
	return fmt.Errorf("upload reattach not supported for this destination")
}

func columnIndex(columns []string, name string) int {
	for i, c := range columns {
		if c == name {
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

type synapseChunkProgress struct {
	hourIndex      int
	subIndex       int
	sortKey        string
	pendingMaxKey  string
	lastPartNum    int
	rowsProcessed  int64
	bytesProcessed int64
}

func (p *Pipeline) writeSynapseStreamCheckpointFromProgress(destBucket, outputKey string, totalHours int, progress *synapseChunkProgress) error {
	return p.writeSynapseStreamCheckpoint(
		destBucket,
		outputKey,
		totalHours,
		progress.hourIndex,
		progress.subIndex,
		progress.sortKey,
		progress.lastPartNum,
		progress.rowsProcessed,
		progress.bytesProcessed,
	)
}

func (p *Pipeline) writeSynapseStreamCheckpoint(destBucket, outputKey string, totalHours, hourIdx, subIdx int, sortKey string, partNum int, rows, bytes int64) error {
	if p.config.EFSMountPath == "" {
		return nil
	}
	cp := state.Checkpoint{
		Stage:                state.StageStreaming,
		UploadID:             p.uploader.UploadID(),
		Bucket:               destBucket,
		Key:                  outputKey,
		RowsProcessed:        rows,
		BytesProcessed:       bytes,
		LastUploadedPart:     partNum,
		SynapseHourIndex:     hourIdx,
		SynapseSubChunkIndex: subIdx,
		SynapseLastSortKey:   sortKey,
		TotalHourChunks:      totalHours,
	}
	if azureUp, ok := p.uploader.(*upload.AzureUploader); ok {
		if partNum > 0 {
			cp.UploadBlockIDs = upload.BlockIDsThrough(partNum)
		} else if existing, err := azureUp.ListParts(context.Background()); err == nil && len(existing) > 0 {
			cp.UploadBlockIDs = upload.BlockIDsThrough(len(existing))
		}
	}
	return state.Write(p.config.EFSMountPath, p.reportID, cp)
}
