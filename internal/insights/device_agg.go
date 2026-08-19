package insights

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/databahn-ai/common-utils/utils"
	osstore "github.com/databahn-ai/databahn-jobs/internal/store/os"
	"github.com/databahn-ai/go-logging/logger"
	"github.com/mitchellh/mapstructure"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"go.uber.org/zap"
)

const deviceScript = `
{
  "script": {
    "source": "
      if (ctx._source.sources == null) { ctx._source.sources = new ArrayList(); }
      boolean found = false;
      for (int i = 0; i < ctx._source.sources.size(); i++) {
        def s = ctx._source.sources[i];
        if (s.source_id == params.source_id) {
          if (s.min_time == null || params.min_time < s.min_time) { s.min_time = params.min_time; }
          if (s.max_time == null || params.max_time > s.max_time) { s.max_time = params.max_time; }
          found = true;
          break;
        }
      }
      if (!found) {
        Map src = new HashMap();
        src.put('source_id', params.source_id);
        src.put('min_time', params.min_time);
        src.put('max_time', params.max_time);
        ctx._source.sources.add(src);
      }
      if (ctx._source.global_min_time == null || params.min_time < ctx._source.global_min_time) {
        ctx._source.global_min_time = params.min_time;
      }
      if (ctx._source.global_max_time == null || params.max_time > ctx._source.global_max_time) {
        ctx._source.global_max_time = params.max_time;
      }
      ctx._source.key1 = params.key1;
      if (params.key2 != null && params.key2 != '') { ctx._source.key2 = params.key2; }
      if (params.key3 != null && params.key3 != '') { ctx._source.key3 = params.key3; }
      ctx._source.tenant_id = params.tenant_id;
      ctx._source.data_plane_id = params.data_plane_id;
      ctx._source.timestamp = params.timestamp;
      ctx._source.updated_at = params.updated_at;
      if (params.key4 != null && params.key4 != '') {
        if (ctx._source.timezone_update_reason == null || ctx._source.timezone_update_reason != 'manual') {
          def currentTz = ctx._source.device_timezone;
          if (currentTz == null || currentTz != params.key4) {
            ctx._source.device_timezone = params.key4;
            ctx._source.timezone_updated_at = params.updated_at;
            ctx._source.timezone_updated_by = params.timezone_updated_by;
            ctx._source.timezone_update_reason = params.timezone_update_reason;
          }
        }
      }
    ",
    "lang": "painless",
    "params": {
      "key1": {{.Key1 | printf "%q"}},
      "key2": {{.Key2 | printf "%q"}},
      "key3": {{.Key3 | printf "%q"}},
      "key4": {{.Key4 | printf "%q"}},
      "source_id": "{{.SourceId}}",
      "tenant_id": "{{.TenantId}}",
      "data_plane_id": "{{.DataPlaneId}}",
      "min_time": {{.MinTime}},
      "max_time": {{.MaxTime}},
      "timestamp": {{.Timestamp}},
      "updated_at": {{.UpdatedAt}},
      "timezone_updated_by": "{{.TimezoneUpdatedBy}}",
      "timezone_update_reason": "{{.TimezoneUpdateReason}}"
    }
  },
  "upsert": {
    "id": {{.Id | printf "%q"}},
    "key1": {{.Key1 | printf "%q"}},
    "key2": {{.Key2 | printf "%q"}},
    "key3": {{.Key3 | printf "%q"}},
    "tenant_id": "{{.TenantId}}",
    "data_plane_id": "{{.DataPlaneId}}",
    "global_min_time": {{.MinTime}},
    "global_max_time": {{.MaxTime}},
    "timestamp": {{.Timestamp}},
    "updated_at": {{.UpdatedAt}},
{{if .Key4}}    "device_timezone": {{.Key4 | printf "%q"}},
    "timezone_updated_at": {{.UpdatedAt}},
    "timezone_updated_by": "{{.TimezoneUpdatedBy}}",
    "timezone_update_reason": "{{.TimezoneUpdateReason}}",
{{end}}    "sources": [
      {
        "source_id": "{{.SourceId}}",
        "min_time": {{.MinTime}},
        "max_time": {{.MaxTime}}
      }
    ]
  }
}
`

type DeviceDocument struct {
	Id                   string `json:"id"`
	Key1                 string `json:"key1"`
	Key2                 string `json:"key2,omitempty"`
	Key3                 string `json:"key3,omitempty"`
	Key4                 string `json:"key4,omitempty"`
	SourceId             string `json:"source_id"`
	TenantId             string `json:"tenant_id"`
	DataPlaneId          string `json:"data_plane_id"`
	MinTime              int64  `json:"min_time"`
	MaxTime              int64  `json:"max_time"`
	Timestamp            int64  `json:"timestamp"`
	UpdatedAt            int64  `json:"updated_at"`
	TimezoneUpdatedBy    string `json:"timezone_updated_by,omitempty"`
	TimezoneUpdateReason string `json:"timezone_update_reason,omitempty"`
}

func backfillDevicesFromSights(ctx context.Context, parallelism int) []JobError {
	logger.GetLogger().Info("device backfill starting", zap.String("mode", "BACKFILL"))

	indices, err := osstore.CatIndices(ctx, osstore.GetClient())
	if err != nil {
		return []JobError{{Message: fmt.Sprintf("device backfill: failed to list indices: %v", err)}}
	}

	sightsIndices := listSourceHostnameSightsIndices(indices)
	logger.GetLogger().Info("device backfill sights indices discovered", zap.Int("index_count", len(sightsIndices)))

	var jobErrors []JobError
	parrCtrl := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	var errorsMutex sync.Mutex

	for _, sightsIndex := range sightsIndices {
		tenantId, err := tenantIdFromSightsIndex(sightsIndex)
		if err != nil {
			logger.GetLogger().Warn("device backfill skipping index", zap.String("index", sightsIndex), zap.Error(err))
			continue
		}

		wg.Add(1)
		parrCtrl <- struct{}{}
		go func(tenantId, sightsIndex string) {
			defer func() {
				<-parrCtrl
				wg.Done()
			}()

			stats, err := backfillDevicesForTenant(ctx, osstore.GetClient(), tenantId, sightsIndex)
			if err != nil {
				errorsMutex.Lock()
				jobErrors = append(jobErrors, JobError{
					Message: fmt.Sprintf("device backfill failed for tenant %s: %v", tenantId, err),
				})
				errorsMutex.Unlock()
				logger.GetLogger().Error("device backfill failed for tenant",
					zap.String("tenant_id", tenantId),
					zap.String("sights_index", sightsIndex),
					zap.Error(err))
				return
			}

			logger.GetLogger().Info("device backfill completed for tenant",
				zap.String("tenant_id", tenantId),
				zap.String("sights_index", sightsIndex),
				zap.String("device_index", DeviceIndexName(tenantId)),
				zap.Int("pages", stats.Pages),
				zap.Int("sights_docs_read", stats.SightsDocsRead),
				zap.Int("device_upserts", stats.DeviceUpserts),
				zap.Int("unique_devices", stats.UniqueDevices))
		}(tenantId, sightsIndex)
	}

	wg.Wait()

	if len(jobErrors) == 0 {
		logger.GetLogger().Info("device backfill finished successfully", zap.Int("tenant_count", len(sightsIndices)))
	} else {
		logger.GetLogger().Info("device backfill finished with errors",
			zap.Int("tenant_count", len(sightsIndices)),
			zap.Int("error_count", len(jobErrors)))
	}

	return jobErrors
}

type deviceBackfillStats struct {
	Pages          int
	SightsDocsRead int
	DeviceUpserts  int
	UniqueDevices  int
}

// streamingUniqueDeviceCounter counts distinct device IDs in sorted order using O(1) memory.
// Input must be sorted by device identity so equal IDs are contiguous across pages.
type streamingUniqueDeviceCounter struct {
	lastDeviceID string
	total        int
}

func (c *streamingUniqueDeviceCounter) observe(deviceID string) bool {
	if deviceID == c.lastDeviceID {
		return false
	}
	c.lastDeviceID = deviceID
	c.total++
	return true
}

func (c *streamingUniqueDeviceCounter) observeBatch(deviceIDs []string) int {
	newInBatch := 0
	for _, deviceID := range deviceIDs {
		if c.observe(deviceID) {
			newInBatch++
		}
	}
	return newInBatch
}

func (c *streamingUniqueDeviceCounter) totalUnique() int {
	return c.total
}

func backfillDevicesForTenant(ctx context.Context, cli *opensearch.Client, tenantId, sightsIndex string) (deviceBackfillStats, error) {
	stats := deviceBackfillStats{}
	uniqueCounter := &streamingUniqueDeviceCounter{}

	query := "*"
	sort := []osstore.Sort{
		{Field: "key1.keyword", Order: "asc"},
		{Field: "key2.keyword", Order: "asc"},
		{Field: "source_id", Order: "asc"},
	}

	var after []any
	page := 0

	logger.GetLogger().Info("device backfill starting for tenant",
		zap.String("tenant_id", tenantId),
		zap.String("sights_index", sightsIndex),
		zap.String("device_index", DeviceIndexName(tenantId)))

	for {
		data, newAfter, err := osstore.SearchPaginated(ctx, cli, sightsIndex, query, getInsightsReadBatch(), after, sort)
		if err != nil {
			return stats, err
		}
		if len(data) == 0 {
			break
		}

		page++
		after = newAfter

		var sights []Sight
		decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{TagName: "json", Result: &sights})
		if err := decoder.Decode(data); err != nil {
			return stats, fmt.Errorf("decode sights page %d: %w", page, err)
		}

		deviceDocs := make([]DeviceDocument, 0, len(sights))
		deviceIDs := make([]string, 0, len(sights))
		for _, sight := range sights {
			doc := sightToDeviceDoc(sight)
			deviceDocs = append(deviceDocs, doc)
			deviceIDs = append(deviceIDs, doc.Id)
		}
		pageUnique := uniqueCounter.observeBatch(deviceIDs)

		written, err := upsertDeviceDocs(ctx, cli, tenantId, deviceDocs)
		if err != nil {
			return stats, fmt.Errorf("upsert page %d: %w", page, err)
		}

		stats.Pages = page
		stats.SightsDocsRead += len(sights)
		stats.DeviceUpserts += written
		stats.UniqueDevices = uniqueCounter.totalUnique()

		logger.GetLogger().Info("device backfill page processed",
			zap.String("tenant_id", tenantId),
			zap.String("sights_index", sightsIndex),
			zap.Int("page", page),
			zap.Int("sights_docs", len(sights)),
			zap.Int("device_upserts", written),
			zap.Int("unique_devices_in_page", pageUnique),
			zap.Int("unique_devices_total", stats.UniqueDevices))
	}

	return stats, nil
}

func sightToDeviceDoc(s Sight) DeviceDocument {
	now := time.Now().UnixMilli()
	ts := s.Timestamp
	if ts == 0 {
		ts = now
	}
	doc := DeviceDocument{
		Id:          DeviceId(s.TenantId, s.Key1),
		Key1:        s.Key1,
		Key2:        s.Key2,
		Key3:        s.Key3,
		SourceId:    s.SourceId,
		TenantId:    s.TenantId,
		DataPlaneId: s.DataPlaneId,
		MinTime:     s.MinTime,
		MaxTime:     s.MaxTime,
		Timestamp:   ts,
		UpdatedAt:   now,
	}
	populateAgentDetectedTimezone(&doc, s.Key4)
	return doc
}

func docToDeviceDoc(d Doc) DeviceDocument {
	now := time.Now().UnixMilli()
	doc := DeviceDocument{
		Id:          DeviceId(d.TenantId, d.Key1),
		Key1:        d.Key1,
		Key2:        d.Key2,
		Key3:        d.Key3,
		SourceId:    d.SourceId,
		TenantId:    d.TenantId,
		DataPlaneId: d.DataPlaneId,
		MinTime:     d.MinTime,
		MaxTime:     d.MaxTime,
		Timestamp:   d.Timestamp,
		UpdatedAt:   now,
	}
	populateAgentDetectedTimezone(&doc, d.Key4)
	return doc
}

func populateAgentDetectedTimezone(doc *DeviceDocument, key4 string) {
	key4 = strings.TrimSpace(key4)
	if key4 == "" || !IsValidIANATimezone(key4) {
		return
	}
	doc.Key4 = key4
	doc.TimezoneUpdatedBy = AgentTimezoneUpdatedByUUID
	doc.TimezoneUpdateReason = TimezoneUpdateReasonAgentSetting
}

func docsToDeviceDocuments(docs []Doc) []DeviceDocument {
	result := make([]DeviceDocument, 0, len(docs))
	for _, d := range docs {
		result = append(result, docToDeviceDoc(d))
	}
	return result
}

func countUniqueDeviceIDs(docs []DeviceDocument) int {
	seen := make(map[string]struct{}, len(docs))
	for _, d := range docs {
		seen[d.Id] = struct{}{}
	}
	return len(seen)
}

func upsertDeviceDocs(ctx context.Context, cli *opensearch.Client, tenantId string, documents []DeviceDocument) (int, error) {
	if len(documents) == 0 {
		return 0, nil
	}

	batchSize := getDeviceAggWriteBatch()
	totalWritten := 0
	for _, batch := range chunkDeviceDocuments(documents, batchSize) {
		written, err := upsertDeviceDocsBatch(ctx, cli, tenantId, batch)
		if err != nil {
			return totalWritten, err
		}
		totalWritten += written
	}

	return totalWritten, nil
}

func chunkDeviceDocuments(documents []DeviceDocument, batchSize int) [][]DeviceDocument {
	if len(documents) == 0 {
		return nil
	}
	if batchSize < 1 {
		batchSize = defaultDeviceAggWriteBatch
	}

	chunks := make([][]DeviceDocument, 0, (len(documents)+batchSize-1)/batchSize)
	for start := 0; start < len(documents); start += batchSize {
		end := start + batchSize
		if end > len(documents) {
			end = len(documents)
		}
		chunks = append(chunks, documents[start:end])
	}
	return chunks
}

func upsertDeviceDocsBatch(ctx context.Context, cli *opensearch.Client, tenantId string, documents []DeviceDocument) (int, error) {
	if len(documents) == 0 {
		return 0, nil
	}

	index := DeviceIndexName(tenantId)
	buff := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := fmt.Fprintf(buff, "{\"update\": {\"_id\": %s}}\n", strconv.Quote(doc.Id))
		if err != nil {
			return 0, err
		}
		j, err := getDeviceUpdateRequestBody(&doc)
		if err != nil {
			return 0, err
		}
		if !isValidJSON(j) {
			logger.GetLogger().Error("invalid json for device doc",
				zap.String("doc_id", doc.Id),
				zap.String("index", index),
				zap.String("json", string(j)))
			return 0, errors.New("invalid json for device doc: " + doc.Id)
		}
		buff.Write(j)
		buff.Write([]byte("\n"))
	}

	request := opensearchapi.BulkRequest{
		Index: index,
		Body:  buff,
	}
	if err := performBulkRequest(ctx, cli, &request); err != nil {
		return 0, err
	}

	logger.GetLogger().Debug("upserted device documents",
		zap.String("tenant_id", tenantId),
		zap.String("index", index),
		zap.Int("count", len(documents)))

	return len(documents), nil
}

func getDeviceUpdateRequestBody(doc *DeviceDocument) ([]byte, error) {
	b := strings.ReplaceAll(deviceScript, "\n", " ")
	return utils.ParseTemplate([]byte(b), doc)
}
