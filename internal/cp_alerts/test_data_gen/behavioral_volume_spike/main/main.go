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

// Hardcoded test data
var tenantId = "f5e31bb8-af80-40d8-a0e4-16f12187e4e4"
var destination1Id = "1bf93a2e-0359-4d32-b158-faeaca46f96a"
var destination2Id = "07b35e56-29fa-4b4c-9902-3879397bc52d"
var dataplaneId = "9bd6eac5-268c-43a6-8554-52486a8822a7"

// Source 1: Normal behavior with sudden spike on last day (yesterday)
var source1Id = "2e730869-b988-436a-a123-48bcf4b3c90f"
var source1IngestionVolumes = []int64{
	10_000_000, // 14 days ago
	11_000_000, // 13 days ago
	10_500_000, // 12 days ago
	10_800_000, // 11 days ago
	10_200_000, // 10 days ago
	10_900_000, // 9 days ago
	10_600_000, // 8 days ago
	10_400_000, // 7 days ago
	10_700_000, // 6 days ago
	10_300_000, // 5 days ago
	10_500_000, // 4 days ago
	10_800_000, // 3 days ago
	10_600_000, // 2 days ago
	50_000_000, // YESTERDAY - SPIKE! (almost 5x normal)
}

// Source 2: Gradual increase (should NOT trigger alert - within normal variance)
var source2Id = "b89e7d5f-abeb-4d17-9b3f-c0c7a25aaa55"
var source2IngestionVolumes = []int64{
	20_000_000, // 14 days ago
	21_000_000, // 13 days ago
	20_500_000, // 12 days ago
	22_000_000, // 11 days ago
	21_500_000, // 10 days ago
	22_500_000, // 9 days ago
	21_800_000, // 8 days ago
	22_200_000, // 7 days ago
	21_900_000, // 6 days ago
	22_800_000, // 5 days ago
	22_500_000, // 4 days ago
	23_000_000, // 3 days ago
	22_700_000, // 2 days ago
	24_000_000, // YESTERDAY - slight increase (within normal range)
}

// Destination 1: Normal behavior with sudden spike on last day (yesterday)
var destination1DeliveryVolumes = []int64{
	9_000_000,  // 14 days ago
	9_500_000,  // 13 days ago
	9_200_000,  // 12 days ago
	9_400_000,  // 11 days ago
	9_100_000,  // 10 days ago
	9_600_000,  // 9 days ago
	9_300_000,  // 8 days ago
	9_200_000,  // 7 days ago
	9_500_000,  // 6 days ago
	9_100_000,  // 5 days ago
	9_400_000,  // 4 days ago
	9_600_000,  // 3 days ago
	9_300_000,  // 2 days ago
	45_000_000, // YESTERDAY - SPIKE! (almost 5x normal)
}

// Destination 2: Gradual increase (should NOT trigger alert - within normal variance)
var destination2DeliveryVolumes = []int64{
	18_000_000, // 14 days ago
	19_000_000, // 13 days ago
	18_500_000, // 12 days ago
	19_500_000, // 11 days ago
	19_200_000, // 10 days ago
	20_000_000, // 9 days ago
	19_800_000, // 8 days ago
	20_200_000, // 7 days ago
	19_900_000, // 6 days ago
	20_500_000, // 5 days ago
	20_300_000, // 4 days ago
	20_800_000, // 3 days ago
	20_600_000, // 2 days ago
	22_000_000, // YESTERDAY - slight increase (within normal range)
}

func main() {
	ctx := context.Background()
	osClient := dbos.GetClient()

	logger.GetLogger().Info("generating behavioral spike test data",
		zap.String("tenantId", tenantId),
		zap.String("source1Id", source1Id),
		zap.String("source2Id", source2Id),
		zap.String("destination1Id", destination1Id),
		zap.String("destination2Id", destination2Id))

	// Generate ingestion data for source 1 (with spike)
	err := generateIngestionDataForSource(ctx, osClient, source1Id, source1IngestionVolumes)
	if err != nil {
		logger.GetLogger().Panic("error generating ingestion data for source 1", zap.Error(err))
	}

	// Generate ingestion data for source 2 (normal gradual increase)
	err = generateIngestionDataForSource(ctx, osClient, source2Id, source2IngestionVolumes)
	if err != nil {
		logger.GetLogger().Panic("error generating ingestion data for source 2", zap.Error(err))
	}

	// Generate delivery data for destination 1 (with spike)
	err = generateDeliveryDataForDestination(ctx, osClient, destination1Id, source1Id, destination1DeliveryVolumes)
	if err != nil {
		logger.GetLogger().Panic("error generating delivery data for destination 1", zap.Error(err))
	}

	// Generate delivery data for destination 2 (normal gradual increase)
	err = generateDeliveryDataForDestination(ctx, osClient, destination2Id, source2Id, destination2DeliveryVolumes)
	if err != nil {
		logger.GetLogger().Panic("error generating delivery data for destination 2", zap.Error(err))
	}

	logger.GetLogger().Info("behavioral spike test data generation complete")
}

func generateIngestionDataForSource(ctx context.Context, osClient *opensearch.Client, sourceId string, ingestionVolumes []int64) error {
	now := time.Now().UTC()
	yesterday := now.Add(-24 * time.Hour)

	numDays := len(ingestionVolumes)
	logger.GetLogger().Info("generating ingestion data for source",
		zap.String("sourceId", sourceId),
		zap.Int("numDays", numDays))

	// Process each day (last element is yesterday, go backwards)
	for i := 0; i < numDays; i++ {
		daysAgo := numDays - 1 - i
		dayDate := yesterday.AddDate(0, 0, -daysAgo)
		dayStart := time.Date(dayDate.Year(), dayDate.Month(), dayDate.Day(), 0, 0, 0, 0, time.UTC)
		dayEnd := dayStart.Add(24 * time.Hour).Add(-1 * time.Millisecond)

		// Get the index for this day
		year := dayDate.Year()
		dayOfYear := dayDate.YearDay()
		indexName := fmt.Sprintf("db_statistics_%s_v2_p1_y%d_d%d", tenantId, year, dayOfYear)

		logger.GetLogger().Info("generating ingestion data for day",
			zap.String("sourceId", sourceId),
			zap.String("date", dayDate.Format("2006-01-02")),
			zap.Int("daysAgo", daysAgo),
			zap.Int64("ingestionVolume", ingestionVolumes[i]),
			zap.String("index", indexName))

		// Generate ingestion documents
		ingestionDocs := generateDocsForDay(
			sourceId,
			"", // No destination needed for ingestion
			ingestionVolumes[i],
			dayStart.UnixMilli(),
			dayEnd.UnixMilli(),
			"total_data_received",
			"storage",
		)

		err := Save(ctx, osClient, ingestionDocs, indexName)
		if err != nil {
			logger.GetLogger().Error("error saving ingestion documents",
				zap.String("sourceId", sourceId),
				zap.String("date", dayDate.Format("2006-01-02")),
				zap.Error(err))
			return err
		}
	}

	return nil
}

func generateDeliveryDataForDestination(ctx context.Context, osClient *opensearch.Client, destinationId string, sourceId string, deliveryVolumes []int64) error {
	now := time.Now().UTC()
	yesterday := now.Add(-24 * time.Hour)

	numDays := len(deliveryVolumes)
	logger.GetLogger().Info("generating delivery data for destination",
		zap.String("destinationId", destinationId),
		zap.String("sourceId", sourceId),
		zap.Int("numDays", numDays))

	// Process each day (last element is yesterday, go backwards)
	for i := 0; i < numDays; i++ {
		daysAgo := numDays - 1 - i
		dayDate := yesterday.AddDate(0, 0, -daysAgo)
		dayStart := time.Date(dayDate.Year(), dayDate.Month(), dayDate.Day(), 0, 0, 0, 0, time.UTC)
		dayEnd := dayStart.Add(24 * time.Hour).Add(-1 * time.Millisecond)

		// Get the index for this day
		year := dayDate.Year()
		dayOfYear := dayDate.YearDay()
		indexName := fmt.Sprintf("db_statistics_%s_v2_p1_y%d_d%d", tenantId, year, dayOfYear)

		logger.GetLogger().Info("generating delivery data for day",
			zap.String("destinationId", destinationId),
			zap.String("sourceId", sourceId),
			zap.String("date", dayDate.Format("2006-01-02")),
			zap.Int("daysAgo", daysAgo),
			zap.Int64("deliveryVolume", deliveryVolumes[i]),
			zap.String("index", indexName))

		// Generate delivery documents
		deliveryDocs := generateDocsForDay(
			sourceId,
			destinationId,
			deliveryVolumes[i],
			dayStart.UnixMilli(),
			dayEnd.UnixMilli(),
			"total_bytes_delivered",
			"dispenser",
		)

		err := Save(ctx, osClient, deliveryDocs, indexName)
		if err != nil {
			logger.GetLogger().Error("error saving delivery documents",
				zap.String("destinationId", destinationId),
				zap.String("sourceId", sourceId),
				zap.String("date", dayDate.Format("2006-01-02")),
				zap.Error(err))
			return err
		}
	}

	return nil
}

func generateDocsForDay(sourceId, destinationId string, totalBytes int64, fromMs, toMs int64, metricName, componentName string) []*Doc {
	var docs []*Doc

	// Split total bytes into multiple events (simulate multiple data points throughout the day)
	// We'll create between 50-100 events per day
	numEvents := 50 + rand.Intn(51)
	bytesPerEvent := totalBytes / int64(numEvents)

	for i := 0; i < numEvents; i++ {
		timestamp := randomTimeBetween(fromMs, toMs)
		doc := createDoc(timestamp, sourceId, destinationId, int(bytesPerEvent), metricName, componentName)
		docs = append(docs, &doc)
	}

	return docs
}

func createDoc(timeStamp int64, sourceId, destinationId string, counter int, name, componentName string) Doc {
	doc := Doc{}
	doc.Name = name
	doc.Namespace = "ingestion-storage"
	doc.Timestamp = timeStamp
	doc.Tags.DbTsWin = timeStamp
	doc.Tags.DbFleetId = ""
	doc.Tags.DbDeviceVendor = "amazon_web_services"
	doc.Tags.DbEventSourceId = sourceId
	doc.Tags.DbDestinationId = destinationId
	doc.Tags.DbDataPlaneId = dataplaneId
	doc.Tags.DbConnectorId = "aws-sqs-connector"
	doc.Tags.ServiceName = "test_service"
	doc.Tags.DbDeviceType = "cloudtrail"
	doc.Tags.ComponentName = componentName
	doc.Counter.Value = counter
	return doc
}

func Save(ctx context.Context, cli *opensearch.Client, documents []*Doc, indexName string) error {
	if len(documents) == 0 {
		return nil
	}
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := fmt.Fprintf(buff, "{\"index\": {\"_index\": \"%s\"}}\n", indexName)
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
	}
	if resp.IsError() {
		return errors.New(resp.String())
	}

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
	logger.GetLogger().Info("indexed documents to opensearch",
		zap.Int("count", len(documents)),
		zap.String("index", indexName))

	return nil
}

type EsResp struct {
	Errors bool `json:"errors"`
}

func randomTimeBetween(from, to int64) int64 {
	if from >= to {
		return from
	}
	return from + rand.Int63n(to-from)
}
