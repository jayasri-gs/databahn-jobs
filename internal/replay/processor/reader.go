package processor

import (
	"bufio"
	"compress/gzip"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/replaymanager"

	"fmt"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"path/filepath"
)

func ReadAndProduce(fileName string, offsetSeek int, mst *replaymanager.MetaDataStore, reqId string, threadId int, topic string) (error, string) {

	filePath := filepath.Join(mst.GetPath("dataDir"), fileName)
	logger.GetLogger().Info(" starting for    ", zap.String("filePath", filePath), zap.String("traceId", reqId), zap.Int("thread ", threadId))
	file, err := os.Open(filePath)
	if err != nil {
		logger.GetLogger().Error(" unable to open file   ", zap.String("filePath", filePath), zap.String("Error", err.Error()), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		return err, constants.StatusFailed
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logger.GetLogger().Error(" unable to close file   ", zap.String("filePath", filePath), zap.String("Error", err.Error()), zap.String("traceId", reqId), zap.Int("thread ", threadId))
			return
		}
	}(file)

	stats, _ := file.Stat()

	mst.UpdateMetaData(fileName, constants.StatusProcessing, 0, 0, stats.Size(), 0, "")
	logger.GetLogger().Info("getting producer", zap.String("traceId", reqId), zap.Int("thread ", threadId))
	var producer = GetProducer(reqId, topic)

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		logger.GetLogger().Error("failed to create gzip reader, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		return err, constants.StatusFailed
	}
	defer gzipReader.Close()

	scanner := bufio.NewScanner(gzipReader)
	if err = scanner.Err(); err != nil {
		logger.GetLogger().Error("error reading file, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		return err, constants.StatusFailed

	}

	var lineSlice string
	lineCounter := 0
	var byteSize int64 = 0
	for scanner.Scan() {
		line := scanner.Text()
		lineSlice = lineSlice + "\n" + line
		byteSize = byteSize + int64(len(line))
		if offsetSeek > lineCounter {
			lineCounter++
			continue
		}
		if offsetSeek == lineCounter {
			logger.GetLogger().Info("seek to line completed", zap.Int("offset", offsetSeek), zap.Int("lineCounter", lineCounter), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		}

		if lineCounter%1000 == 0 {
			message := kafka.Message{
				Message: []byte(lineSlice),
			}
			producer.SendAsync(message, func(err error) {
				logger.GetLogger().Error("error while publishing to kafka", zap.Error(err))
			})
			fmt.Println("lineCounter : ", lineCounter)
			mst.UpdateMetaData(fileName, "", lineCounter, 0, 0, byteSize, "")
		}
		lineCounter++
	}
	mst.UpdateMetaData(fileName, constants.StatusCompleted, lineCounter, 0, 0, byteSize, "")
	logger.GetLogger().Info("completed the process", zap.String("file", fileName), zap.Int("lineCounter", lineCounter), zap.String("traceId", reqId), zap.Int("thread ", threadId))
	replaymanager.CleanUpFile(filePath)

	return nil, ""
}
