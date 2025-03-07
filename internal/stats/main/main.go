package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	dbos "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
	"io"
	"math/rand"
	"time"
)

type Doc struct {
	Tags struct {
		DbTsWin         int64  `json:"db_ts_win"`
		DbFleetId       string `json:"db_fleet_id"`
		DbDeviceVendor  string `json:"db_device_vendor"`
		DbEventSourceId string `json:"db_event_source_id"`
		DbDestinationId string `json:"db_destination_id"`
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

var tenantIds = []string{"a79756ce-6e65-4cd3-830b-1e5134049c6a", "b79756ce-6e65-4cd3-830b-1e5134049c6b"}
var name = "total_data_received"
var namespace = "ingestion-storage"
var db_device_vendor = "amazon_web_services"
var db_data_plane_id = "9bd6eac5-268c-43a6-8554-52486a8822a7"
var db_connector_id = "aws-sqs-connector"
var service_name = "ingestion-storage"
var db_device_type = "cloudtrail"
var component_name = "storage"
var sourceIds = []string{"f188efab-2169-4d07-aa62-c66904fe7511", "f188efab-2169-4d07-aa62-c66904fe7522", "f188efab-2169-4d07-aa62-c66904fe7533"}
var destinationIds = []string{"28c543e4-1b34-44c8-ad16-6d6adfaebc11", "28c543e4-1b34-44c8-ad16-6d6adfaebc22", "28c543e4-1b34-44c8-ad16-6d6adfaebc33"}

func main() {
	ctx := context.Background()
	conf := dbos.GetConf()
	osClient, err := dbos.NewClient(ctx, conf.Url, conf.Creds())
	if err != nil {
		logger.GetLoggerWithContext(ctx).Panic("error while connecting to statistics store", zap.Error(err))
	}
	thisYear, thisDay := getYearAndDay()
	updateOlderDays := 6
	olderDays := 4
	for i := olderDays; i < updateOlderDays; i++ {
		year, day := thisYear, thisDay-i
		if day <= 0 {
			year, day = year-1, day+365
		}
		tenantId := randomItem(tenantIds)
		indexName := fmt.Sprintf("db_statistics_%s_v2_p1_y%d_d%d", tenantId, year, day)
		logger.GetLogger().Info("creating for year and day", zap.Int("year", year), zap.Int("day", day), zap.String("index", indexName))
		dayEpochStart, dayEpochEnd := getEpochTimeRange(year, day)
		startRange := dayEpochStart + (10 * 60 * 1000)
		endRange := dayEpochEnd - (10 * 60 * 1000)
		twentySeconds := int64(20 * 1000)
		var docs []*Doc
		for timeStamp := startRange; timeStamp < endRange; {
			sourceId := randomItem(sourceIds)
			destinationId := randomItem(destinationIds)
			count := 1 + rand.Intn(5)
			for j := 0; j < count; j++ {
				doc := randomDoc(tenantId, timeStamp, sourceId, destinationId)
				docs = append(docs, &doc)
			}
			if timeStamp%3600000 == 0 {
				err := Save(ctx, osClient, docs, indexName)
				if err != nil {
					logger.GetLogger().Panic("error while saving documents", zap.Error(err))
				}
				docs = nil
			}
			timeStamp = timeStamp + twentySeconds
		}
		if len(docs) > 0 {
			err := Save(ctx, osClient, docs, indexName)
			if err != nil {
				logger.GetLogger().Panic("error while saving documents", zap.Error(err))
			}
		}
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

func randomDoc(tenant string, timeStamp int64, source, destination string) Doc {
	doc := Doc{}
	doc.Name = name
	doc.Namespace = namespace
	doc.Timestamp = timeStamp
	doc.Tags.DbTsWin = timeStamp
	doc.Tags.DbFleetId = ""
	doc.Tags.DbDeviceVendor = db_device_vendor
	doc.Tags.DbEventSourceId = source
	doc.Tags.DbDestinationId = destination
	doc.Tags.DbDataPlaneId = db_data_plane_id
	doc.Tags.DbConnectorId = db_connector_id
	doc.Tags.ServiceName = service_name
	doc.Tags.DbDeviceType = db_device_type
	doc.Tags.ComponentName = component_name
	doc.Counter.Value = 1 + rand.Intn(500)
	return doc
}

func randomItem(items []string) string {
	return items[rand.Intn(len(items))]
}

func getYearAndDay() (int, int) {
	now := time.Now()
	year := now.Year()
	day := now.YearDay()
	return year, day
}

func getEpochTimeRange(year, day int) (int64, int64) {
	yearStart := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
	dayDate := yearStart.AddDate(0, 0, day-1)
	start := dayDate.UnixMilli()
	end := dayDate.AddDate(0, 0, 1).UnixMilli()
	return start, end
}

func yearWeekNumber(year, week int) int {
	return year*100 + week
}

func yearDayNumber(year, day int) int {
	return year*1000 + day
}

func splitYearWeekNumber(yearWeek int) (int, int) {
	year := yearWeek / 100
	week := yearWeek % 100
	return year, week
}

func yearWeekFromYearDay(year, dayOfYear int) (int, int) {
	startOfYear := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
	date := startOfYear.AddDate(0, 0, dayOfYear-1)
	year, week := date.ISOWeek()
	return year, week
}
