package replaymanager

import (
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
	"time"
)

func PreProcessMetaData(input model.Message, jobName string, mst *MetaDataStore) (error, bool, int) {

	var metaDataFile *os.File
	logger.GetLogger().Info("pre-processing starting", zap.String("traceId", input.RequestId), zap.Int("thread ", -1))

	_, err := os.Stat(mst.metaFilePath)
	//FRESH JOB RUN FIRST TIME
	if os.IsNotExist(err) {
		logger.GetLogger().Info("baseDir does not exist! creating request folder", zap.String("dir ", mst.baseDir), zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
		errDir := os.MkdirAll(mst.metaDir, 0777)

		if errDir != nil {
			logger.GetLogger().Error(" not able to create dir", zap.String("dir", mst.metaDir), zap.Error(errDir), zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
			return errDir, true, 1
		}
		metaDataFile, errDir = os.Create(mst.metaFilePath)
		defer func(metaDataFile *os.File) {
			err := metaDataFile.Close()
			if err != nil {
				logger.GetLogger().Error(" error while closing dir", zap.String("dir", mst.metaDir), zap.Error(err), zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
			}
		}(metaDataFile)

		if errDir != nil {
			logger.GetLogger().Error("not able to open file, exiting path:  ", zap.String("dir", mst.metaFilePath), zap.Error(errDir), zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
			return errDir, true, 1
		}

		var metaObject model.MetaDataValue

		metaObject = model.MetaDataValue{
			FileName:  constants.Global,
			Key:       constants.Global,
			Offset:    0,
			Retry:     0,
			JobName:   jobName,
			RequestId: input.RequestId,
			Status:    constants.StatusYetToProcess,

			Time: time.Now(),
		}

		mst.AddMetaData(metaObject, constants.Global)

		for _, file := range input.FileName {

			if file == "" {
				logger.GetLogger().Warn(" skipping empty file from the list", zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
				continue
			}

			metaObject = model.MetaDataValue{
				FileName:  file,
				Key:       file,
				Offset:    0,
				Retry:     1,
				JobName:   jobName,
				RequestId: input.RequestId,
				Status:    constants.StatusYetToProcess,

				Time: time.Now(),
			}

			mst.AddMetaData(metaObject, file)
			mst.AddToProcessList(file)
		}

		//Sending file as global and rest field empty for flushing data
		mst.UpdateMetaData(constants.Global, "", 0, 0, 0, 0, "")

	} else { // IN-CASE OF JOB RESUME SCENARIO

		var errDir error
		metaDataFile, errDir = os.Open(mst.metaFilePath)
		defer func(metaDataFile *os.File) {
			if metaDataFile != nil {
				err := metaDataFile.Close()
				if err != nil {
					logger.GetLogger().Error(" Error:  ", zap.String("dir", mst.metaFilePath), zap.Error(err), zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
				}
			}

		}(metaDataFile)
		if errDir != nil {
			logger.GetLogger().Error(" not able to open file, exiting path:  ", zap.String("dir", mst.metaFilePath), zap.Error(errDir), zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
			return errDir, true, 1

		}

		bytesData, errDir := io.ReadAll(metaDataFile)
		if errDir != nil {
			logger.GetLogger().Error(" error reading metaData.json:  ", zap.String("dir", mst.metaDir), zap.Error(errDir), zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
			return errDir, true, 1
		}
		errDir = json.Unmarshal(bytesData, &mst.metaMap)
		if errDir != nil {
			logger.GetLogger().Error(" error unmarshal metaData.json:  ", zap.String("dir", mst.metaDir), zap.Error(errDir), zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
			return errDir, true, 1
		}

		global := mst.metaMap[constants.Global]
		if global.Status == constants.StatusCompleted {
			logger.GetLogger().Info("all files are processed , exiting", zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
			return nil, true, 0
		}
		for key, data := range mst.metaMap {
			if key == "GLOBAL" {
				fmt.Println("GLOBAL is here , GLOBAL ")
				continue
			}
			if (!strings.Contains(data.Status, constants.StatusCompleted)) || !(data.Retry > constants.MaxRetry) {
				mst.AddToProcessList(key)
				mst.UpdateMetaData(key, "", 0, 1, 0, 0, "")
			}
		}
	}
	logger.GetLogger().Info("pre-processing completed.", zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
	if len(mst.processList) == 0 {
		logger.GetLogger().Info("there are 0 files exist so Exiting...", zap.String("traceId", input.RequestId), zap.Int("thread ", -1))
		mst.UpdateMetaData(constants.Global, constants.StatusCompleted, 0, 0, 0, 0, "")
		return nil, true, 0
	}
	return nil, false, -1
}
