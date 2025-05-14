package processor

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/databahn-ai/common-utils/ack"
	commConst "github.com/databahn-ai/common-utils/constants"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/common-utils/utils"
	"github.com/databahn-ai/databahn-jobs/internal/replay/constants"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/replay/replaymanager"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type DatabahnParsedData struct {
	RawEvent string `json:"rawevent"`
}

func ReadAndProduce(fileName string, offsetSeek int, mst *replaymanager.MetaDataStore, reqId string, threadId int, topic string, req model.Message, throughPutController *util.ThroughputController) (error, string) {

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

	var scanner *bufio.Scanner

	switch strings.ToLower(req.DataStore) {
	case constants.S3_STORAGE_TYPE, "":
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			if err.Error() == "gzip: invalid header" {
				newErr := fmt.Errorf(fileName + " :- file is not gZip ")
				logger.GetLogger().Error("failed to create gzip reader, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
				return newErr, constants.StatusFailed
			}

			logger.GetLogger().Error("failed to create gzip reader, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		}
		defer func(gzipReader *gzip.Reader) {
			err := gzipReader.Close()
			if err != nil {
				logger.GetLogger().Error("Error while closing gzip reader", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
			}
		}(gzipReader)

		scanner = bufio.NewScanner(gzipReader)
		if err = scanner.Err(); err != nil {
			logger.GetLogger().Error("error reading file, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
			return err, constants.StatusFailed
		}
	case constants.AZURE_BLOB_STORAGE_TYPE:
		scanner = bufio.NewScanner(file)
		if err = scanner.Err(); err != nil {
			logger.GetLogger().Error("error reading file, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
			return err, constants.StatusFailed
		}
	default:
		logger.GetLogger().Error("unsupported file type", zap.String("fileType", req.DataStore), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		return fmt.Errorf("unsupported file type"), constants.StatusFailed
	}

	//var lineSlice string
	lineCounter := 0
	var byteSize int64 = 0
	forwardDataType := req.AdditionalConfig["forward_data_type"]

	for scanner.Scan() {
		line := scanner.Text()
		//lineSlice = lineSlice + "\n" + line
		byteSize = byteSize + int64(len(line))
		if offsetSeek > lineCounter {
			lineCounter++
			continue
		}
		if offsetSeek == lineCounter {
			logger.GetLogger().Info("seek to line completed", zap.Int("offset", offsetSeek), zap.Int("lineCounter", lineCounter), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		}

		if strings.ToLower(forwardDataType) == "parsed" {
			line, err = getRawDataFromDataBahnParsedObject(line)
			if err != nil {
				return err, constants.StatusFailed
			}
		}
		message := kafka.Message{
			Message: []byte(line),
			Headers: GetHeader(req),
		}
		throughPutController.IncrementOrWait()
		producer.SendAsyncTopic(message, utils.GetDynamicTopicName(topic), func(err error) {
			logger.GetLogger().Error("error while publishing to kafka", zap.Error(err))
		})

		if lineCounter%10000 == 0 {
			//logger.GetLogger(("lineCounter : ", lineCounter))
			mst.UpdateMetaData(fileName, "", lineCounter, 0, 0, byteSize, "")
		}
		lineCounter++
	}
	//waiting for 5 sec to flush  before completion
	count := producer.Producer.Flush(10000)
	logger.GetLogger().Info("Flushed the events", zap.String("file", fileName), zap.Int("event pending", count), zap.String("traceId", reqId), zap.Int("thread ", threadId))
	mst.UpdateMetaData(fileName, constants.StatusCompleted, lineCounter, 0, 0, byteSize, "")
	logger.GetLogger().Info("completed the process", zap.String("file", fileName), zap.Int("lineCounter", lineCounter), zap.String("traceId", reqId), zap.Int("thread ", threadId))
	replaymanager.CleanUpFile(filePath)

	return nil, ""
}

func ProduceStatus(mst *replaymanager.MetaDataStore, inputReq model.Message) {

	var statusList []ack.Status
	for _, val := range mst.GetMetaMap() {
		filepath.Join(val.Prefix, val.FileName)

		status := ack.Status{
			FileName:    val.FileName,
			RequestId:   val.RequestId,
			Status:      val.Status,
			FileSize:    val.FileSize,
			FilePath:    filepath.Join(val.Prefix, val.FileName),
			CurrentSize: val.CurrentSize,
			ErrorMsg:    val.ErrorMsg,
			StartTime:   val.Time,
			EndTime:     val.EndTime,
			Percentage:  0.0,
			Lines:       val.Offset,
		}
		statusList = append(statusList, status)
	}

	acknowledgment := PrepareAck(statusList, inputReq)
	err := AckProducer.Produce(context.Background(), acknowledgment, nil)
	if err != nil {
		logger.GetLogger().Error("error while publishing status to kafka", zap.Error(err))
		return
	}
	logger.GetLogger().Info(" publishing status to kafka")

}

func PrepareAck(status []ack.Status, inputReq model.Message) ack.Ack {
	return ack.Ack{
		Type:        "REPLAY",
		EntityId:    inputReq.RequestId,
		RequestId:   inputReq.AckId,
		TenantId:    inputReq.TenantId,
		Action:      "Create",
		EntityType:  "data-replay",
		Status:      "completed",
		ServiceName: utils.GetEnvOrDefault("SERVICE_NAME", "data-replay"),
		ReplayStatus: &ack.ReplayStatus{
			Status: status,
		},
	}
}

func GetHeader(request model.Message) []kafka.Header {

	headers := make([]kafka.Header, 13)
	headers[0] = kafka.Header{Key: commConst.DeviceType, Value: []byte(request.DeviceType)}
	headers[1] = kafka.Header{Key: commConst.DeviceVendor, Value: []byte(request.DeviceVendor)}
	headers[2] = kafka.Header{Key: commConst.LogType, Value: []byte(request.LogType)}
	headers[3] = kafka.Header{Key: commConst.TenantId, Value: []byte(request.TenantId)}
	headers[4] = kafka.Header{Key: commConst.EventSourceId, Value: []byte(request.Source)}
	headers[5] = kafka.Header{Key: commConst.EdgeId, Value: []byte(uuid.Nil.String())}
	headers[6] = kafka.Header{Key: commConst.FleetId, Value: []byte(request.FleetId)}
	headers[7] = kafka.Header{Key: commConst.ConnectorId, Value: []byte(request.Source)}
	headers[8] = kafka.Header{Key: commConst.EventId, Value: []byte(uuid.NewString())}
	headers[9] = kafka.Header{Key: commConst.EdgeTimestamp, Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))}
	headers[10] = kafka.Header{Key: "db_component_name", Value: []byte("replay_data")}
	headers[11] = kafka.Header{Key: commConst.PipelineDone, Value: []byte(commConst.DataReplayStage)}
	headers[12] = kafka.Header{Key: commConst.SourceName, Value: []byte(request.SourceName)}

	return headers
}

func getRawDataFromDataBahnParsedObject(line string) (string, error) {
	var parsedData DatabahnParsedData
	if err := json.Unmarshal([]byte(line), &parsedData); err != nil {
		err = fmt.Errorf("failed to unmarshal Parsed event to extract rawevent: %v", err)
		return "", err
	}
	return parsedData.RawEvent, nil
}
