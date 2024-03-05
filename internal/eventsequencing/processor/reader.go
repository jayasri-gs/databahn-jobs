package processor

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/common-utils/kafka"
	"github.com/databahn-ai/databahn-jobs/internal/config"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/constants"
	modele "github.com/databahn-ai/databahn-jobs/internal/eventsequencing/model"
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/replaymanager"
	"github.com/databahn-ai/databahn-jobs/internal/replay/model"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func ReadAndProduce(fileName string, offsetSeek int, mst *replaymanager.MetaDataStore, reqId string, threadId int, topic string, req model.Message, sst *replaymanager.SortStore) (error, string) {

	prefix := mst.GetMetaMap()[fileName].Prefix
	tenant, source, destination := ExtractDetails(prefix)
	outerKey := sst.GetOuterkey(tenant, source, destination)

	logger.GetLogger().Info(" starting for    ", zap.String("outerKey", outerKey), zap.String("traceId", reqId), zap.Int("thread ", threadId))
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
	//	var producer = GetProducer(reqId, topic)

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
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	//var lineSlice string
	lineCounter := 0
	var byteSize int64 = 0

	for scanner.Scan() {
		line := scanner.Text()

		byteSize = byteSize + int64(len(line))
		if offsetSeek > lineCounter {
			lineCounter++
			continue
		}
		if offsetSeek == lineCounter {
			logger.GetLogger().Info("seek to line completed", zap.Int("offset", offsetSeek), zap.Int("lineCounter", lineCounter), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		}

		rawEvent := modele.RawEvent{}
		err := json.Unmarshal([]byte(line), &rawEvent)
		if err != nil {
			logger.GetLogger().Error("error while unmarshalling the data", zap.Error(err))
			return err, ""
		}
		// Convert the string to an int64
		num, err := strconv.ParseInt(rawEvent.Headers["db_edge_ts"], 10, 64)
		if err != nil {
			logger.GetLogger().Error("error while converting string to int64", zap.Error(err))
			continue
		}
		rawEvent.EventTime = num
		eventsArray := sst.Get(outerKey)
		eventsArray = append(eventsArray, rawEvent)
		sst.AddToSortStore(outerKey, eventsArray)
		if lineCounter%10000 == 0 {

			mst.UpdateMetaData(fileName, "", lineCounter, 0, 0, byteSize, "")
		}
		lineCounter++
	}

	if err = scanner.Err(); err != nil {
		logger.GetLogger().Error("error reading file, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		return err, constants.StatusFailed
	}
	if PrintDataInSequence(sst, outerKey) != nil {
		logger.GetLogger().Error("error sending data to destination, %v", zap.Error(err), zap.String("traceId", reqId), zap.Int("thread ", threadId))
		return err, constants.StatusFailed
	}
	mst.UpdateMetaData(fileName, constants.StatusCompleted, lineCounter, 0, 0, byteSize, "")
	logger.GetLogger().Info("completed the process", zap.String("file", fileName), zap.Int("lineCounter", lineCounter), zap.String("traceId", reqId), zap.Int("thread ", threadId))
	replaymanager.CleanUpFile(filePath)

	return nil, ""
}

func ExtractDetails(sequence string) (string, string, string) {
	parts := strings.Split(sequence, "/")

	tenant := strings.Split(parts[1], "=")[1]
	source := strings.Split(parts[2], "=")[1]
	destination := strings.Split(parts[3], "=")[1]
	return tenant, source, destination
}

func PrintDataInSequence(sst *replaymanager.SortStore, outerKey string) error {

	sstStore := sst.GetSortStore()
	eventsArray := sstStore[outerKey]
	logger.GetLogger().Info("Sorting Size of Array :  ", zap.String("UniqueKey", outerKey), zap.Int("Size", len(eventsArray)))
	logger.GetLogger().Info("Processing send message in sequence for ", zap.String("UniqueKey", outerKey))

	confDestination := config.GetDestinationConfiguration()
	confDest := confDestination.GetStringMapString(configuration.DestinationTopicMapping)

	var producer = GetProducer("reqId")

	sort.SliceStable(eventsArray, func(i, j int) bool {
		return eventsArray[i].EventTime < eventsArray[j].EventTime
	})

	for _, val := range eventsArray {

		message := kafka.Message{
			Key:     []byte(outerKey),
			Message: []byte(val.Body),
			Headers: GetHeaderFromMsg1(val.Headers),
		}
		dType, ok := val.Headers[constants.DestinationType]
		fmt.Println("Destination Type For : ", dType)
		if !ok {
			logger.GetLogger().Error("error while fetching destination type from the headers")
			continue
		}
		topic, ok := confDest[strings.ToLower(dType)]
		fmt.Println("topic is  : ", topic)
		if ok {
			producer.SendAsyncTopic(message, topic, func(err error) {
				if err != nil {
					logger.GetLogger().Error("error while sending message to kafka", zap.Error(err))
				} else {
					fmt.Println("DATA SENT TO KAFKA########: " + topic)
				}
			})
		} else {
			logger.GetLogger().Error("error while fetching destination topic from the DestinationTopicMapping")
		}

	}
	producer.Producer.Flush(10000)
	logger.GetLogger().Info(fmt.Sprintf("Data Flushed for producer"))
	time.Sleep(5 * time.Second)
	sst.DeleteSStData(outerKey)

	return nil
}

func GetHeaderFromMsg1(request map[string]string) []kafka.Header {
	headers := make([]kafka.Header, 6)
	headers[0] = kafka.Header{Key: constants.RuleId, Value: []byte(request[constants.RuleId])}
	headers[1] = kafka.Header{Key: constants.DestinationId, Value: []byte(request[constants.DestinationId])}
	headers[2] = kafka.Header{Key: constants.EventId, Value: []byte(request[constants.EventId])}
	headers[3] = kafka.Header{Key: constants.EdgeTimestamp, Value: []byte(request[constants.EdgeTimestamp])}
	headers[4] = kafka.Header{Key: constants.DestinationType, Value: []byte(request[constants.DestinationType])}
	headers[5] = kafka.Header{Key: constants.EventSourceId, Value: []byte(request[constants.EventSourceId])}
	return headers
}

func GetHeaderFromMsg(request map[string]string) []kafka.Header {
	var headers []kafka.Header
	keys := []string{constants.DeviceType, constants.DeviceVendor, constants.LogType, constants.TenantID,
		constants.EventSourceId, constants.EdgeId, constants.FleetId, constants.ConnectorId,
		constants.EventId, constants.EdgeTimestamp, constants.ComponentName, constants.DestinationType,
		constants.PipelineId, constants.RuleId, constants.DestinationId}

	for _, key := range keys {
		if value, ok := request[key]; ok {
			headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
		}
	}

	return headers
}
