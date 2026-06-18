package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	"github.com/databahn-ai/databahn-jobs/internal/searchExport/state"
	"go.uber.org/zap"
)

func (p *Pipeline) buildExportObjectKey(ext string) (fileName, outputKey string) {
	fileName = exportFileName(p.exportName, p.request.TableName, p.request.DataSetName, ext, p.reportID)
	outputKey = fmt.Sprintf("exports/%s/%s", p.reportID, fileName)
	return fileName, outputKey
}

func (p *Pipeline) runSynapseStreamExport(ctx context.Context, destBucket string, syn query.RowStreamExecutor) (*PipelineResult, error) {
	p.log.Info("Running Synapse JDBC stream export")

	if err := syn.ValidateExportQuery(ctx, p.request.Query); err != nil {
		return nil, err
	}

	columns, err := syn.GetQueryColumns(ctx, p.request.Query, p.request.Database)
	if err != nil {
		return nil, err
	}

	exportFormat := normalizedFormat(p.request.Format)
	ext, contentType := formatMeta(exportFormat)
	fileName, outputKey := p.buildExportObjectKey(ext)
	p.log.Info("Export output file", zap.String("fileName", fileName), zap.String("outputKey", outputKey))

	if err := p.uploader.Init(ctx, destBucket, outputKey, contentType); err != nil {
		return nil, fmt.Errorf("failed to init uploader: %w", err)
	}

	opts := query.StreamRowsOptionsFromEnv()

	totalRows, totalBytes, err := p.encodeRowsToUploader(ctx, func() []string { return columns }, func(cb func([]interface{}) error) error {
		_, streamErr := syn.StreamRows(ctx, p.request.Query, opts, cb)
		return streamErr
	}, func(partNum int, rows, bytes int64) error {
		if p.config.EFSMountPath == "" {
			return nil
		}
		return state.Write(p.config.EFSMountPath, p.reportID, state.Checkpoint{
			Stage:            state.StageStreaming,
			UploadID:         p.uploader.UploadID(),
			Bucket:           destBucket,
			Key:              outputKey,
			RowsProcessed:    rows,
			BytesProcessed:   bytes,
			LastUploadedPart: partNum,
		})
	})
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
