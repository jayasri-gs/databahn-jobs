package replaymanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
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

func (mst *MetaDataStore) GetMetaData(key string) (model.MetaDataValue, bool) {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	val, ok := mst.metaMap[key]
	return val, ok
}

func (mst *MetaDataStore) GetProcessList() []string {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	out := make([]string, len(mst.processList))
	copy(out, mst.processList)
	return out
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
	defer mst.Mutex.Unlock()
	mst.metaMap[key] = value
}

func (mst *MetaDataStore) DeleteMetaData(key string) {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	delete(mst.metaMap, key)
}

func (mst *MetaDataStore) UpdateMetaData(key string, status string, offset int, retry int, fileSize int64, currentSize int64, errorMsg string) {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
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
	mst.flushLocked()
}

func (mst *MetaDataStore) TimeStampMetaData(key string, start bool, end bool) {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	data := mst.metaMap[key]
	if start {
		data.Time = time.Now()
	}
	if end {
		data.EndTime = time.Now()
	}
	mst.metaMap[key] = data
	mst.flushLocked()
}

func (mst *MetaDataStore) AddToProcessList(value string) {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	mst.processList = append(mst.processList, value)
}

func (mst *MetaDataStore) GetValuesOfMap() []model.MetaDataValue {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	metaMapValues := make([]model.MetaDataValue, 0, len(mst.metaMap))
	for _, val := range mst.metaMap {
		metaMapValues = append(metaMapValues, val)
	}
	return metaMapValues
}

func (mst *MetaDataStore) loadMetaMapFromJSON(bytesData []byte) error {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	return json.Unmarshal(bytesData, &mst.metaMap)
}

func (mst *MetaDataStore) Flush() {
	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	mst.flushLocked()
}

// flushLocked persists metaMap to disk. Caller must hold mst.Mutex.
func (mst *MetaDataStore) flushLocked() {
	if len(mst.metaMap) == 0 {
		logger.GetLogger().Info(fmt.Sprintf(" Can not Write MetaData.json: with size 0 , Skipping Write"))
		return
	}

	bytesData, err := json.Marshal(mst.metaMap)
	if err != nil {
		logger.GetLogger().Error(" Marshalling Error MetaData.json:  ", zap.Error(err))
		return
	}

	err = os.WriteFile(mst.metaFilePath, bytesData, 0777)
	if err != nil {
		logger.GetLogger().Error(" Error Writing MetaData.json:  ", zap.Error(err))
	}
}

func (mst *MetaDataStore) UpdateGlobalStatus() {
	logger.GetLogger().Info("CleanUp Invoked.")

	mst.Mutex.Lock()
	defer mst.Mutex.Unlock()
	success := 0
	failed := 0
	for key, data := range mst.metaMap {
		if key == constants.Global {
			continue
		}

		switch data.Status {
		case constants.StatusCompleted:
			success++
		case constants.StatusFailed:
			failed++
		}
	}
	global := mst.metaMap[constants.Global]

	totalFiles := len(mst.metaMap) - 1

	if totalFiles > 0 && failed == totalFiles {
		global.Status = constants.StatusFailed
	} else if success == totalFiles {
		global.Status = constants.StatusCompleted
	} else if failed+success == totalFiles {
		global.Status = constants.CompletedWithError
	} else {
		global.Status = constants.StatusInProgress
	}

	global.Time = time.Now()
	mst.metaMap[constants.Global] = global
	mst.flushLocked()
	logger.GetLogger().Info("CleanUp StatusCompleted.")
}
