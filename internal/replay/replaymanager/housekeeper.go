package replaymanager

import (
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
)

func CleanUpFile(file string) {
	e := os.Remove(file)
	if e != nil {
		logger.GetLogger().Error(" error while deleting File :  ", zap.Error(e))
	}
	logger.GetLogger().Info("file deleted successfully", zap.String("fileName", file))
}
