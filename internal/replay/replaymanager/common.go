package replaymanager

import (
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type MetaDataStore struct {
	sync.Mutex
	metaMap      map[string]model.MetaDataValue
	processList  []string
	baseDir      string
	dataDir      string
	metaDir      string
	metaFilePath string
}

func (mst *MetaDataStore) GetMetaMap() map[string]model.MetaDataValue {
	return mst.metaMap
}

func (mst *MetaDataStore) GetProcessList() []string {
	return mst.processList
}
func (mst *MetaDataStore) GetPath(path string) string {

	switch path {

	case "baseDir":
		return mst.baseDir
	case "metaDir":
		return mst.metaDir
	case "dataDir":
		return mst.dataDir
	case "metaFile":
		return mst.metaFilePath
	default:
		return mst.baseDir
	}

}

func NewMetaStore(reqId string) (*MetaDataStore, error) {

	location := utils.GetEnvOrDefault(constants.MountLocationKey, constants.MountLocation)
	basePath := filepath.Join(location, reqId)
	metaPath := filepath.Join(basePath, constants.MetaDir)
	logger.GetLogger().Info("PATH: ", zap.String("location", location), zap.String("BasePath", basePath), zap.String("metaPath", metaPath))
	mst := MetaDataStore{
		metaMap:      make(map[string]model.MetaDataValue),
		baseDir:      basePath,
		metaDir:      metaPath,
		dataDir:      filepath.Join(basePath, constants.DataDir),
		metaFilePath: filepath.Join(metaPath, constants.MetaJson),
	}

	return &mst, nil
}

func (mst *MetaDataStore) AddMetaData(value model.MetaDataValue, key string) {
	mst.Mutex.Lock()
	mst.metaMap[key] = value
	mst.Mutex.Unlock()
}

func (mst *MetaDataStore) DeleteMetaData(key string) {
	mst.Mutex.Lock()
	delete(mst.metaMap, key)
	mst.Mutex.Unlock()
}

func (mst *MetaDataStore) UpdateMetaData(key string, status string, offset int, retry int, fileSize int64, currentSize int64, errorMsg string) {
	mst.Mutex.Lock()
	data := mst.metaMap[key]
	if offset != 0 {
		data.Offset = offset
	}
	if retry != 0 && constants.MaxRetry >= data.Retry {
		data.Retry = data.Retry + retry
	}
	if status != "" {
		data.Status = status
	}
	if errorMsg != "" {
		data.ErrorMsg = append(data.ErrorMsg, errorMsg)
	}
	if fileSize != 0 {
		data.FileSize = fileSize
	}
	if currentSize != 0 {
		data.CurrentSize = currentSize

	}
	mst.metaMap[key] = data
	mst.Mutex.Unlock()
	mst.Flush()

}
func (mst *MetaDataStore) TimeStampMetaData(key string, start bool, end bool) {
	mst.Mutex.Lock()
	data := mst.metaMap[key]
	if start {
		data.Time = time.Now()
	}
	if end {
		data.EndTime = time.Now()
	}
	mst.metaMap[key] = data
	mst.Mutex.Unlock()
	mst.Flush()

}

func (mst *MetaDataStore) AddToProcessList(value string) {
	mst.Mutex.Lock()
	mst.processList = append(mst.processList, value)
	mst.Mutex.Unlock()

}
func (mst *MetaDataStore) GetValuesOfMap() []model.MetaDataValue {
	metaMapValues := make([]model.MetaDataValue, 0, len(mst.GetMetaMap()))
	for _, val := range mst.metaMap {
		metaMapValues = append(metaMapValues, val)
	}
	return metaMapValues
}

func (mst *MetaDataStore) Flush() {

	if len(mst.metaMap) == 0 {
		logger.GetLogger().Info(fmt.Sprintf(" Can not Write MetaData.json: with size 0 , Skipping Write"))
		return
	}

	mst.Lock()

	_, err := json.Marshal(mst.metaMap)
	if err != nil {
		logger.GetLogger().Error(" Marshalling Error MetaData.json:  ", zap.Error(err))
		return
	}

	bytesData, _ := json.Marshal(mst.metaMap)

	mst.Unlock()

	err = os.WriteFile(mst.metaFilePath, bytesData, 7777)
	if err != nil {
		logger.GetLogger().Error(" Error Writing MetaData.json:  ", zap.Error(err))
		return
	}

}

func (mst *MetaDataStore) UpdateGlobalStatus() {

	logger.GetLogger().Info("CleanUp Invoked.")
	success := 0
	failed := 0
	partial := 0
	for key, data := range mst.metaMap {

		if key == constants.Global {
			continue
		}

		switch data.Status {

		case constants.StatusCompleted:
			success++
		case constants.StatusFailed:
			failed++
		case constants.StatusDownloadFailed, constants.CompletedWithError, constants.StatusDownloaded:
			partial++
		}
	}
	global := mst.metaMap[constants.Global]

	totalFiles := len(mst.metaMap) - 1

	if (failed == (totalFiles)) || (success == 0 && failed == 0 && partial > 0) {
		global.Status = constants.StatusFailed

	} else if success == (totalFiles) {
		global.Status = constants.StatusCompleted

	} else if success > 0 && (failed > 0 || partial > 0) {
		global.Status = constants.CompletedWithError
	}

	global.Time = time.Now()
	mst.metaMap[constants.Global] = global
	//key string, status string, offset int, retry int, fileSize int64, currentSize int64, errorMsg string
	mst.UpdateMetaData(constants.Global, "", 0, 0, 0, 0, "")
	logger.GetLogger().Info("CleanUp StatusCompleted.")
	return
}
