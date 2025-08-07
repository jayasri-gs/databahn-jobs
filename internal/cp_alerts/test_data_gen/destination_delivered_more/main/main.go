package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"time"

	dbos "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
)

type Doc struct {
	Tags struct {
		DbTsWin         int64  `json:"db_ts_win"`
		DbFleetId       string `json:"db_fleet_id"`
		DbDeviceVendor  string `json:"db_device_vendor"`
		DbEventSourceId string `json:"db_event_source_id"`
		DbDestinationId string `json:"destination_id"`
		DbDataPlaneId   string `json:"db_data_plane_id"`
		DbConnectorId   string `json:"db_connector_id"`
		ServiceName     string `json:"service_name"`
		DbDeviceType    string `json:"db_device_type"`
		ComponentName   string `json:"component_name"`
	} `json:"tags"`
	Counter struct {
		Value int `json:"value"`
	} `json:"counter"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Timestamp int64  `json:"timestamp"`
}

var tenantId = "f5e31bb8-af80-40d8-a0e4-16f12187e4e4"
var sourceIdsToCountsForInput = map[string]int{
	"2e730869-b988-436a-a123-48bcf4b3c90f": 100,
	"b89e7d5f-abeb-4d17-9b3f-c0c7a25aaa55": 100,
}
var sourceIdsToCountsForOutput = map[string]int{
	"2e730869-b988-436a-a123-48bcf4b3c90f": 106,
	"b89e7d5f-abeb-4d17-9b3f-c0c7a25aaa55": 108,
}
var destinationId = "444d43f6-7378-46b1-96d0-0ba4e63ecf81"

func main() {
	ctx := context.Background()
	osClient := dbos.GetClient()
	thisYear, thisDay := getYearAndDay()

	eventByteSize := 1000
	indexName := fmt.Sprintf("db_statistics_%s_v2_p1_y%d_d%d", tenantId, thisYear, thisDay)
	logger.GetLogger().Info("creating for year and day", zap.Int("year", thisYear), zap.Int("day", thisDay), zap.String("index", indexName))

	to := time.Now().Add(-1 * time.Hour).UnixMilli()
	from := time.Now().Add(-3 * time.Hour).UnixMilli()

	var docs []*Doc
	for sourceId, count := range sourceIdsToCountsForOutput {
		for i := 0; i < count; i++ {
			timeStamp := randomTimeBetween(from, to)
			doc := randomDoc(timeStamp, sourceId, destinationId, eventByteSize, "total_bytes_delivered", "dispenser")
			docs = append(docs, &doc)
		}
	}
	err := Save(ctx, osClient, docs, indexName)
	if err != nil {
		logger.GetLogger().Panic("error while saving documents", zap.Error(err))
	}

	docs = nil
	for sourceId, count := range sourceIdsToCountsForInput {
		for i := 0; i < count; i++ {
			timeStamp := randomTimeBetween(from, to)
			doc := randomDoc(timeStamp, sourceId, destinationId, eventByteSize, "total_data_received", "storage")
			docs = append(docs, &doc)
		}
	}
	err = Save(ctx, osClient, docs, indexName)
	if err != nil {
		logger.GetLogger().Panic("error while saving documents", zap.Error(err))
	}

}

func Save(ctx context.Context, cli *opensearch.Client, documents []*Doc, indexName string) error {
	if len(documents) == 0 {
		return nil
	}
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := fmt.Fprintf(buff, "{\"index\": {\"_index\": \"%s\"}}\n", indexName)
		if err != nil {
			panic(err)
		}

		if err != nil {
			return err
		}
		j, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		buff.Write(j)
		buff.Write([]byte("\n"))
	}
	request := opensearchapi.BulkRequest{
		Index: indexName,
		Body:  buff,
	}
	resp, err := request.Do(ctx, cli)
	if err != nil {
		return err
	} else {
		if resp.IsError() {
			return errors.New(resp.String())
		} else {
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			bodyJ := EsResp{}
			err = json.Unmarshal(bodyBytes, &bodyJ)
			if err != nil {
				return err
			}
			if bodyJ.Errors {
				return errors.New(string(bodyBytes))
			}
			logger.GetLogger().Info("indexed documents to es", zap.Int("count", len(documents)), zap.String("index", indexName))
		}
	}
	return nil
}

type EsResp struct {
	Errors bool `json:"errors"`
}

func randomDoc(timeStamp int64, source, destination string, counter int, name, componentName string) Doc {
	doc := Doc{}
	doc.Name = name
	doc.Namespace = "ingestion-storage"
	doc.Timestamp = timeStamp
	doc.Tags.DbTsWin = timeStamp
	doc.Tags.DbFleetId = ""
	doc.Tags.DbDeviceVendor = "amazon_web_services"
	doc.Tags.DbEventSourceId = source
	doc.Tags.DbDestinationId = destination
	doc.Tags.DbDataPlaneId = "9bd6eac5-268c-43a6-8554-52486a8822a7"
	doc.Tags.DbConnectorId = "aws-sqs-connector"
	doc.Tags.ServiceName = "any_service"
	doc.Tags.DbDeviceType = "cloudtrail"
	doc.Tags.ComponentName = componentName
	doc.Counter.Value = counter
	return doc
}

func randomItem(items []string) string {
	return items[rand.Intn(len(items))]
}

func randomTimeBetween(from, to int64) int64 {
	return from + rand.Int63n(to-from)
}

func getYearAndDay() (int, int) {
	now := time.Now()
	year := now.Year()
	day := now.YearDay()
	return year, day
}
