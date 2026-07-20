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
	"github.com/databahn-ai/databahn-jobs/internal/replay/metrics"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/databahn-jobs/internal/replay/replaymanager"
	"github.com/databahn-ai/databahn-jobs/internal/util"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type databahnParsedLine struct {
	RawEvent json.RawMessage `json:"rawevent"`
}

type databahnRawEventObject struct {
	Msg string `json:"msg"`
}

type Scanner interface {
	Scan() bool
	Text() string
	Err() error
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
	forwardDataType := req.AdditionalConfig["forward_data_type"]
	isParquetFile := false

	mst.UpdateMetaData(fileName, constants.StatusProcessing, 0, 0, stats.Size(), 0, "")
	logger.GetLogger().Info("getting producer", zap.String("traceId", reqId), zap.Int("thread ", threadId))
	var producer = GetProducer(reqId, topic)
	logger.GetLogger().Info("getting producer for topic", zap.String("topic", topic), zap.String("fileName", fileName),
		zap.String("replayType", req.ReplayType))
	if matchReplayType(req.ReplayType, "UNDELIVERED") {
		logger.GetLogger().Info("pipeline done and next", zap.String("pipeline_done", req.AdditionalHeaders[commConst.PipelineDone]),
			zap.String("pipeline_next", req.AdditionalHeaders[commConst.PipelineNext]))
	}
	var scanner Scanner

	switch strings.ToLower(req.DataStore) {
	case constants.S3_STORAGE_TYPE, "":
		compression := req.AdditionalConfig["compression"]
		if strings.ToLower(compression) == "gzip" {
			// Try gzip decompression first
			gzipReader, err := gzip.NewReader(file)
			if err != nil {
				if err.Error() == "gzip: invalid header" {
					newErr := fmt.Errorf("%s :- file is not gZip ", fileName)
					logger.GetLogger().Error("failed to create gzip reader, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
					return newErr, constants.StatusFailed
				}
				logger.GetLogger().Error("failed to create gzip reader, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
				return err, constants.StatusFailed
			}
			defer func(gzipReader *gzip.Reader) {
				err := gzipReader.Close()
				if err != nil {
					logger.GetLogger().Error("Error while closing gzip reader", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
				}
			}(gzipReader)
			scanner = bufio.NewScanner(gzipReader)
		} else {
			// No compression, read file directly
			scanner = bufio.NewScanner(file)
		}
		if err = scanner.Err(); err != nil {
			logger.GetLogger().Error("error reading file", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
			return err, constants.StatusFailed
		}
	case constants.AZURE_BLOB_STORAGE_TYPE:
		// Check if file is parquet by extension
		if strings.HasSuffix(strings.ToLower(fileName), ".parquet") {
			isParquetFile = true
			parquetScanner, err := NewParquetScanner(file, forwardDataType)
			if err != nil {
				logger.GetLogger().Error("error creating parquet scanner", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
				return err, constants.StatusFailed
			}
			defer parquetScanner.Close()
			if parquetScanner.Salvaged() {
				logger.GetLogger().Warn(
					"processing truncated parquet via salvage reader",
					zap.String("fileName", fileName),
					zap.Int64("salvagedRows", parquetScanner.NumRows()),
					zap.String("traceId", reqId),
					zap.Int("thread ", threadId),
				)
			}
			scanner = parquetScanner
		} else if strings.HasSuffix(strings.ToLower(fileName), ".gz") {
			logger.GetLogger().Info("auto-detected gzip compression from Azure Blob filename",
				zap.String("fileName", fileName),
				zap.String("traceId", reqId))
			gzipReader, err := gzip.NewReader(file)
			if err != nil {
				logger.GetLogger().Error("failed to create gzip reader for Azure Blob file", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
				return err, constants.StatusFailed
			}
			defer func(gzipReader *gzip.Reader) {
				err := gzipReader.Close()
				if err != nil {
					logger.GetLogger().Error("Error while closing gzip reader", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
				}
			}(gzipReader)
			scanner = bufio.NewScanner(gzipReader)
		} else {
			scanner = bufio.NewScanner(file)
		}
		if err = scanner.Err(); err != nil {
			logger.GetLogger().Error("error reading file", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
			return err, constants.StatusFailed
		}
	default:
		logger.GetLogger().Error("unsupported file type", zap.String("fileType", req.DataStore), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		return fmt.Errorf("unsupported file type"), constants.StatusFailed
	}

	//var lineSlice string
	lineCounter := 0
	var byteSize int64 = 0

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

		line, err = prepareReplayLine(line, req, isParquetFile)
		if err != nil {
			return err, constants.StatusFailed
		}
		message := kafka.Message{
			Message: []byte(line),
			Headers: GetHeader(req),
		}
		throughPutController.IncrementOrWait()
		replayTags := metrics.ReplayMetricTags(req)
		recordReplayMetric(metrics.MetricReplayAttempted, replayTags)
		syncFailed := false
		producer.SendAsyncTopic(message, utils.GetDynamicTopicName(topic), func(err error) {
			if err != nil {
				syncFailed = true
				logger.GetLogger().Error("error while publishing to kafka", zap.Error(err))
				recordReplayMetric(metrics.MetricReplayFailed, replayTags)
			}
		})
		if !syncFailed {
			recordReplayMetric(metrics.MetricReplaySucceeded, replayTags)
		}

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
	for _, val := range mst.GetValuesOfMap() {
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

	pipelineDone := commConst.DataReplayStage
	pipelineNext := ""
	destinationId := ""
	pipelineId := ""
	if matchReplayType(request.ReplayType, "UNDELIVERED") {
		pipelineDone = request.AdditionalHeaders[commConst.PipelineDone]
		pipelineNext = request.AdditionalHeaders[commConst.PipelineNext]
		destinationId = request.AdditionalHeaders[commConst.DestinationId]
		pipelineId = request.AdditionalHeaders[commConst.PipelineId]
	}
	headers := make([]kafka.Header, 16)
	headers[0] = kafka.Header{Key: commConst.DeviceType, Value: []byte(request.DeviceType)}
	headers[1] = kafka.Header{Key: commConst.DeviceVendor, Value: []byte(request.DeviceVendor)}
	headers[2] = kafka.Header{Key: commConst.LogType, Value: []byte(request.LogType)}
	headers[3] = kafka.Header{Key: commConst.TenantId, Value: []byte(request.TenantId)}
	headers[4] = kafka.Header{Key: commConst.EventSourceId, Value: []byte(request.Source)}
	headers[5] = kafka.Header{Key: commConst.EdgeId, Value: []byte(uuid.Nil.String())}
	headers[6] = kafka.Header{Key: commConst.FleetId, Value: []byte(request.FleetId)}
	headers[7] = kafka.Header{Key: commConst.ConnectorId, Value: []byte(request.ConnectId)}
	headers[8] = kafka.Header{Key: commConst.EventId, Value: []byte(uuid.NewString())}
	headers[9] = kafka.Header{Key: commConst.EdgeTimestamp, Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))}
	headers[10] = kafka.Header{Key: "db_component_name", Value: []byte("replay_data")}
	headers[11] = kafka.Header{Key: commConst.PipelineDone, Value: []byte(pipelineDone)}
	headers[12] = kafka.Header{Key: commConst.PipelineNext, Value: []byte(pipelineNext)}
	headers[13] = kafka.Header{Key: commConst.SourceName, Value: []byte(request.SourceName)}
	headers[14] = kafka.Header{Key: commConst.DestinationId, Value: []byte(destinationId)}
	headers[15] = kafka.Header{Key: commConst.PipelineId, Value: []byte(pipelineId)}

	headers = append(headers,
		kafka.Header{Key: constants.TrafficTypeHeader, Value: []byte(constants.TrafficTypeReplay)},
		kafka.Header{Key: constants.ReplayTypeHeader, Value: []byte(constants.ReplayTypeFromJobType(request.ReplayType))},
	)
	if request.RequestId != "" {
		headers = append(headers, kafka.Header{Key: constants.ReplayJobIdHeader, Value: []byte(request.RequestId)})
	}

	return headers
}

func prepareReplayLine(line string, req model.Message, isParquetFile bool) (string, error) {
	forwardDataType := req.AdditionalConfig["forward_data_type"]

	switch {
	case matchReplayType(req.ReplayType, "UNDELIVERED"):
		return line, nil

	case matchReplayType(req.ReplayType, "UNPARSED"):
		return getRawDataFromUnparsedObject(line)

	case matchReplayType(req.ReplayType, "CUSTOM"), strings.TrimSpace(req.ReplayType) == "":
		if isParquetFile {
			return line, nil
		}
		if strings.EqualFold(forwardDataType, "parsed") {
			return getRawDataFromDataBahnParsedObject(line)
		}
		return line, nil

	default:
		if isParquetFile {
			return line, nil
		}
		if strings.EqualFold(forwardDataType, "parsed") {
			return getRawDataFromDataBahnParsedObject(line)
		}
		return line, nil
	}
}

func getRawDataFromUnparsedObject(line string) (string, error) {
	return model.ExtractUnparsedRawLog(line)
}

func getRawDataFromDataBahnParsedObject(line string) (string, error) {
	return extractRawEvent(line, "Parsed")
}

func extractRawEvent(line string, eventKind string) (string, error) {
	var parsedLine databahnParsedLine
	if err := json.Unmarshal([]byte(line), &parsedLine); err != nil {
		return "", fmt.Errorf("failed to unmarshal %s event to extract rawevent: %v", eventKind, err)
	}
	if len(parsedLine.RawEvent) == 0 {
		return "", fmt.Errorf("failed to unmarshal %s event to extract rawevent: rawevent is missing or empty", eventKind)
	}

	var raweventString string
	if err := json.Unmarshal(parsedLine.RawEvent, &raweventString); err == nil {
		if raweventString == "" {
			return "", fmt.Errorf("failed to unmarshal %s event to extract rawevent: rawevent is missing or empty", eventKind)
		}
		return raweventString, nil
	}

	var raweventObject databahnRawEventObject
	if err := json.Unmarshal(parsedLine.RawEvent, &raweventObject); err == nil && raweventObject.Msg != "" {
		return raweventObject.Msg, nil
	}

	return "", fmt.Errorf(
		"failed to unmarshal %s event to extract rawevent: rawevent must be a string or an object with msg",
		eventKind,
	)
}

func recordReplayMetric(metricName string, tags map[string]string) {
	metrics.RecordCounter(metricName, tags, 1)
}
