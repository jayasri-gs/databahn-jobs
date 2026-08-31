package pipeline

import (
	"context"

	"github.com/databahn-ai/databahn-jobs/internal/searchExport/query"
	logging "github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

// CleanupADXStaging cancels a still-running .export and deletes the blobs it staged.
//
// Called only after the final retry — an earlier attempt's staged output is reusable, and
// a completed upload already deletes what it read. This covers the failures that happen
// before that point (the export itself failing part-way, listing or upload init failing),
// which would otherwise leave databahn_export_* blobs in the export destination's own
// container. Best effort throughout: every step logs and moves on.
func CleanupADXStaging(ctx context.Context, exec query.UnloadExecutor, reportID, operationID, tempDir string, log *zap.Logger) {
	if exec == nil {
		return
	}
	if log == nil {
		log = logging.GetLogger()
	}

	if operationID != "" {
		if err := exec.Connect(ctx); err != nil {
			log.Warn("ADX permanent-failure cleanup: connect failed", zap.Error(err))
		} else if err := exec.CancelQueryExecution(ctx, operationID); err != nil {
			// Expected when the operation already finished; only a running one is stoppable.
			log.Warn("ADX permanent-failure cleanup: cancel operation failed",
				zap.String("adxOperationId", operationID), zap.Error(err))
		}
	}

	reader, err := exec.NewStagingReader(tempDir)
	if err != nil {
		log.Warn("ADX permanent-failure cleanup: staging reader failed", zap.Error(err))
		return
	}
	prefix := query.ADXNamePrefix(reportID)
	files, err := reader.ListFiles(ctx, prefix)
	if err != nil {
		log.Warn("ADX permanent-failure cleanup: list staged blobs failed", zap.Error(err))
		return
	}
	if len(files) == 0 {
		return
	}
	if err := reader.DeleteFiles(ctx, files); err != nil {
		log.Warn("ADX permanent-failure cleanup: delete staged blobs failed", zap.Error(err))
		return
	}
	log.Info("Deleted ADX staged blobs after permanent failure",
		zap.Int("count", len(files)), zap.String("namePrefix", prefix))
}
